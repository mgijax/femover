# Module: HomologyUtils.py
# Purpose: provides various utility functions to facilitate working with
#       homology data

import gc
import logger
import dbAgnostic
import VocabUtils

###--- Globals ---###

HOMOLOGY = 9272150              # term key for "homology" cluster type
HYBRID = 13764519               # term key for "hybrid" cluster source
HGNC = 13437099                 # term key for "HGNC" cluster source
HOMOLOGENE = 9272151            # term key for "HomoloGene" cluster source
CLUSTER = 39                    # MGI Type key for "Cluster"

HYBRID_CLUSTERS = {}            # marker key : hybrid cluster key
HGNC_CLUSTERS = {}              # marker key : HGNC cluster key
HOMOLOGENE_CLUSTERS = {}        # marker key : HomoloGene cluster key

CLUSTER_SOURCES = {}            # cluster key : source key

HOMOLOGENE_IDS = {}             # HomoloGene cluster key : HomoloGene ID

CACHED_SOURCES = {}             # term key for cached source : 1

DERIVED_FROM = {}               # cluster key : (string) name of source

###--- Functions ---###

def getHybridClusterKeys(markerKey):
        # Returns: list of cluster keys (hybrid source) which contain the
        #       given mouse or human markerKey

        if HYBRID not in CACHED_SOURCES:
                _loadSource(HYBRID) 
        return _lookup(HYBRID_CLUSTERS, markerKey, [])

def getHomoloGeneClusterKeys(markerKey):
        # Returns: list of cluster keys (HomoloGene source) which contain the
        #       given mouse or human markerKey

        if HOMOLOGENE not in CACHED_SOURCES:
                _loadSource(HOMOLOGENE) 
        return _lookup(HOMOLOGENE_CLUSTERS, markerKey, [])

def getHgncClusterKeys(markerKey):
        # Returns: list of cluster keys (HGNC source) which contain the given
        #       mouse or human markerKey

        if HGNC not in CACHED_SOURCES:
                _loadSource(HGNC) 
        return _lookup(HGNC_CLUSTERS, markerKey, [])

def getHomoloGeneID(hgClusterKey):
        # Returns: string accession ID for the HomoloGene cluster with the
        #       given key

        if HOMOLOGENE not in CACHED_SOURCES:
                _loadSource(HOMOLOGENE) 
        return _lookup(HOMOLOGENE_IDS, hgClusterKey, None)

def getSource(clusterKey):
        # Returns: string name of the source of the cluster with the given key

        if clusterKey in CLUSTER_SOURCES:
                return VocabUtils.getTerm(CLUSTER_SOURCES[clusterKey])

        for source in [ HYBRID, HOMOLOGENE, HGNC ]:
                _loadSource(source)

                if clusterKey in CLUSTER_SOURCES:
                        return VocabUtils.getTerm(CLUSTER_SOURCES[clusterKey])
        return None

def getSourceOfCluster(hybridClusterKey):
        # Returns: source name for the cluster which was chosen as the basis
        #       of the hybrid cluster with the specified key

        if 'derived' not in CACHED_SOURCES:
                _loadSourceClusters()

        return _lookup(DERIVED_FROM, hybridClusterKey, None) 

def getMaxClusterKey():
        # Returns: maximum _Cluster_key from the database

        cmd = 'select max(_Cluster_key) from MRK_Cluster'
        cols, rows = dbAgnostic.execute(cmd)

        logger.debug('Found max(_Cluster_key) = %d' % rows[0][0])
        return rows[0][0]

def getMarkersPerCluster():
        # Returns: { cluster key : [ marker keys ] }
        # Note: This is for the HGNC and HomoloGene clusters that were chosen
        #       by the hybrid algorithm, not the hybrid clusters themselves.

        for source in [ HOMOLOGENE, HGNC, HYBRID ]:
                _loadSource(source)

        clusters = {}

        for markerKey in list(HYBRID_CLUSTERS.keys()):
                if getSourceOfCluster(HYBRID_CLUSTERS[markerKey][-1]) == 'HGNC':
                        srcKeys = getHgncClusterKeys(markerKey)
                else:
                        srcKeys = getHomoloGeneClusterKeys(markerKey)

                if not srcKeys:
                        continue

                srcKey = srcKeys[-1]
                if srcKey in clusters:
                        clusters[srcKey].append(markerKey)
                else:
                        clusters[srcKey] = [ markerKey ]

        logger.debug('Grouped %d markers into %d clusters' % (
                len(HYBRID_CLUSTERS), len(clusters)) )
        return clusters

def _lookup(dictionary, key, default = None):
        # Purpose: convenience function to look up and return the value
        #       associated with 'key' in the given 'dictionary', or return the
        #       given 'default' value if the 'key' is not in the 'dictionary'.
        
        if key in dictionary:
                return dictionary[key]
        return default

def _loadSourceClusters():
        # Effects: loads data about what is the source cluster for each hybrid
        #       homology cluster

        global DERIVED_FROM

        if 'derived' in CACHED_SOURCES:
                return
        CACHED_SOURCES['derived'] = 1

        cmd = '''select p.value, mc._Cluster_key
                from MRK_ClusterMember mcm,
                        MRK_Cluster mc,
                        MRK_Marker m,
                        MGI_Property p,
                        VOC_Term t
                where m._Organism_key in (1,2)
                        and m._Marker_key = mcm._Marker_key
                        and mcm._Cluster_key = mc._Cluster_key
                        and mc._ClusterSource_key = %d
                        and mc._ClusterType_key = %d
                        and mc._Cluster_key = p._Object_key
                        and p._MGIType_key = %d
                        and p._PropertyTerm_key = t._Term_key
                        and t.term = 'secondary source' ''' % (
                                HYBRID, HOMOLOGY, CLUSTER)

        cols, rows = dbAgnostic.execute(cmd)

        srcCol = dbAgnostic.columnNumber(cols, 'value')
        clusterCol = dbAgnostic.columnNumber(cols, '_Cluster_key')

        for row in rows:
                DERIVED_FROM[row[clusterCol]] = row[srcCol]

        logger.debug('Got sources for %d clusters' % len(DERIVED_FROM))
        return

def _loadSource(sourceKey):
        # Effects: loads the cluster/marker relationships for the given
        #       homology source into memory

        global CACHED_SOURCES, HYBRID_CLUSTERS, HGNC_CLUSTERS
        global HOMOLOGENE_CLUSTERS, HOMOLOGENE_IDS, CLUSTER_SOURCES

        # if we've already cached this source, skip it

        if sourceKey in CACHED_SOURCES:
                return
        CACHED_SOURCES[sourceKey] = 1

        # get the cluster/marker pairs from the database, with HomoloGene IDs
        # being optional

        cmd = '''select distinct c._Cluster_key, c._Marker_key,
                        a.accID as homologene_id
                from MRK_Cluster mc
                inner join MRK_ClusterMember c on (
                        c._Cluster_key = mc._Cluster_key)
                inner join MRK_Marker m on (c._Marker_key = m._Marker_key
                        and m._Organism_key in (1,2))
                left outer join ACC_Accession a on (
                        mc._Cluster_key = a._Object_key
                        and a.preferred = 1
                        and a._LogicalDB_key = 81)
                where mc._ClusterType_key = %d
                        and mc._ClusterSource_key = %d''' % (HOMOLOGY,sourceKey)

        cols, rows = dbAgnostic.execute(cmd)

        logger.debug('Got %d homology rows for %s' % (len(rows),
                VocabUtils.getTerm(sourceKey)) )

        clusterCol = dbAgnostic.columnNumber(cols, '_Cluster_key')
        markerCol = dbAgnostic.columnNumber(cols, '_Marker_key')
        idCol = dbAgnostic.columnNumber(cols, 'homologene_id')

        # pick which global dictionary to populate, based on the source

        if sourceKey == HYBRID:
                d = HYBRID_CLUSTERS
        elif sourceKey == HGNC:
                d = HGNC_CLUSTERS
        elif sourceKey == HOMOLOGENE:
                d = HOMOLOGENE_CLUSTERS

        for row in rows:
                clusterKey = row[clusterCol]
                markerKey = row[markerCol]

                # always note the source of this cluster

                CLUSTER_SOURCES[clusterKey] = sourceKey

                # track the cluster keys for each marker (usually, but not
                # always 1-to-1)

                if markerKey in d:
                        d[markerKey].append(clusterKey)
                else:
                        d[markerKey] = [ clusterKey ]

                # for HomoloGene, we also need to pick up the IDs

                if sourceKey == HOMOLOGENE:
                        HOMOLOGENE_IDS[clusterKey] = row[idCol]

        logger.debug('Got %s cluster data for %d markers' % (
                VocabUtils.getTerm(sourceKey), len(d)) )
        return

def unload():
        global CACHED_SOURCES, HYBRID_CLUSTERS, HGNC_CLUSTERS, DERIVED_FROM
        global HOMOLOGENE_CLUSTERS, HOMOLOGENE_IDS, CLUSTER_SOURCES

        HYBRID_CLUSTERS = {}
        HGNC_CLUSTERS = {}
        HOMOLOGENE_CLUSTERS = {}
        HOMOLOGENE_IDS = {}
        CACHED_SOURCES = {}
        CLUSTER_SOURCES = {}
        DERIVED_FROM = {}
        gc.collect()
        logger.debug('Unloaded caches in HomologyUtils')
        return
