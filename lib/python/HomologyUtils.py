# Module: HomologyUtils.py
# Purpose: provides various utility functions to facilitate working with
#       homology data

import gc
import logger
import dbAgnostic
import VocabUtils

###--- Globals ---###

HOMOLOGY = 9272150              # term key for "homology" cluster type
ALLIANCE_DIRECT = 75885739      # term key for "Alliance Direct" cluster source
ALLIANCE_CLUSTERED = 75885740   # term key for "Alliance Clustered" cluster source
CLUSTER = 39                    # MGI Type key for "Cluster"

ALLIANCE_DIRECT_CLUSTERS = {}      # marker key : cluster key for Alliance Direct clusters
ALLIANCE_CLUSTERED_CLUSTERS = {}   # marker key : Alliance Clustered cluster key

CLUSTER_SOURCES = {}            # cluster key : source key

CACHED_SOURCES = {}             # term key for cached source : 1

DERIVED_FROM = {}               # cluster key : (string) name of source

###--- Functions ---###

def getAllianceDirectClusterKeys(markerKey):
        # Returns: list of cluster keys (Alliance Direct source) which contain the
        #       given mouse or human markerKey

        if ALLIANCE_DIRECT not in CACHED_SOURCES:
                _loadSource(ALLIANCE_DIRECT) 
        return _lookup(ALLIANCE_DIRECT_CLUSTERS, markerKey, [])

def getAllianceClusteredClusterKeys(markerKey):
        # Returns: list of cluster keys (Alliance Clustered source) which contain the given
        #       mouse or human markerKey

        if ALLIANCE_CLUSTERED not in CACHED_SOURCES:
                _loadSource(ALLIANCE_CLUSTERED) 
        return _lookup(ALLIANCE_CLUSTERED_CLUSTERS, markerKey, [])

def getSource(clusterKey):
        # Returns: string name of the source of the cluster with the given key

        if clusterKey in CLUSTER_SOURCES:
                return VocabUtils.getTerm(CLUSTER_SOURCES[clusterKey])

        for source in [ ALLIANCE_DIRECT, ALLIANCE_CLUSTERED ]:
                _loadSource(source)

                if clusterKey in CLUSTER_SOURCES:
                        return VocabUtils.getTerm(CLUSTER_SOURCES[clusterKey])
        return None

def getSourceOfCluster(clusterKey):
        # Returns: source name for the cluster which was chosen as the basis
        #       of the Alliance Direct cluster with the specified key

        if 'derived' not in CACHED_SOURCES:
                _loadSourceClusters()

        return _lookup(DERIVED_FROM, clusterKey, None) 

def getMaxClusterKey():
        # Returns: maximum _Cluster_key from the database

        cmd = 'select max(_Cluster_key) from MRK_Cluster'
        cols, rows = dbAgnostic.execute(cmd)

        logger.debug('Found max(_Cluster_key) = %d' % rows[0][0])
        return rows[0][0]

def getMarkersPerCluster(clusterSource):
        # Returns: { cluster key : [ marker keys ] }
        # Note: clusterSource should be ALLIANCE_DIRECT or ALLIANCE_CLUSTERED.

        markerKeys = None       # list of marker keys to process
        lookupFn = None         # function to look up cluster keys for a given marker key

        if clusterSource == ALLIANCE_CLUSTERED:
            markerKeys = list(ALLIANCE_CLUSTERED_CLUSTERS.keys())
            lookupFn = getAllianceClusteredClusterKeys

        elif clusterSource == ALLIANCE_DIRECT:
            markerKeys = list(ALLIANCE_DIRECT_CLUSTERS.keys())
            lookupFn = getAllianceDirectClusterKeys

        else:
            raise Exception('Unknown clusterSource: %s' % str(clusterSource))

        clusters = {}

        for markerKey in markerKeys:
                srcKeys = lookupFn(markerKey)
                if not srcKeys:
                        continue

                srcKey = srcKeys[-1]
                if srcKey in clusters:
                        clusters[srcKey].append(markerKey)
                else:
                        clusters[srcKey] = [ markerKey ]

        logger.debug('Grouped %d markers into %d clusters' % (len(markerKeys), len(clusters)) )
        return clusters

def _lookup(dictionary, key, default = None):
        # Purpose: convenience function to look up and return the value
        #       associated with 'key' in the given 'dictionary', or return the
        #       given 'default' value if the 'key' is not in the 'dictionary'.
        
        if key in dictionary:
                return dictionary[key]
        return default

def _loadSourceClusters():
        # Effects: loads data about what is the source cluster for each Alliance Direct
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
                                ALLIANCE_DIRECT, HOMOLOGY, CLUSTER)

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

        global CACHED_SOURCES, ALLIANCE_DIRECT_CLUSTERS, ALLIANCE_CLUSTERED_CLUSTERS_CLUSTERS
        global CLUSTER_SOURCES

        # if we've already cached this source, skip it

        if sourceKey in CACHED_SOURCES:
                return
        CACHED_SOURCES[sourceKey] = 1

        # get the cluster/marker pairs from the database

        cmd = '''select distinct c._Cluster_key, c._Marker_key
                from MRK_Cluster mc
                inner join MRK_ClusterMember c on (
                        c._Cluster_key = mc._Cluster_key)
                inner join MRK_Marker m on (c._Marker_key = m._Marker_key
                        and m._Organism_key in (1,2))
                where mc._ClusterType_key = %d
                        and mc._ClusterSource_key = %d''' % (HOMOLOGY, sourceKey)

        cols, rows = dbAgnostic.execute(cmd)

        logger.debug('Got %d homology rows for %s' % (len(rows),
                VocabUtils.getTerm(sourceKey)) )

        clusterCol = dbAgnostic.columnNumber(cols, '_Cluster_key')
        markerCol = dbAgnostic.columnNumber(cols, '_Marker_key')

        # pick which global dictionary to populate, based on the source

        if sourceKey == ALLIANCE_DIRECT:
                d = ALLIANCE_DIRECT_CLUSTERS
        elif sourceKey == ALLIANCE_CLUSTERED:
                d = ALLIANCE_CLUSTERED_CLUSTERS_CLUSTERS

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

        logger.debug('Got %s cluster data for %d markers' % (
                VocabUtils.getTerm(sourceKey), len(d)) )
        return

def unload():
        global CACHED_SOURCES, ALLIANCE_DIRECT_CLUSTERS, ALLIANCE_CLUSTERED_CLUSTERS_CLUSTERS, DERIVED_FROM
        global CLUSTER_SOURCES

        ALLIANCE_DIRECT_CLUSTERS = {}
        ALLIANCE_CLUSTERED_CLUSTERS_CLUSTERS = {}
        CACHED_SOURCES = {}
        CLUSTER_SOURCES = {}
        DERIVED_FROM = {}
        gc.collect()
        logger.debug('Unloaded caches in HomologyUtils')
        return
