#!./python
# 
# gathers data for the homology_cluster, homology_cluster_organism, and
# homology_cluster_organism_to_marker tables in the front-end database

import Gatherer
import symbolsort
import utils
import logger
import GOGraphs

ALLIANCE_DIRECT = 75885739
ALLIANCE_CLUSTERED = 75885740
HOMOLOGY = 9272150

###--- Globals ---###

# which organisms should be given priority when ordering
preferredOrganisms = [
        # the big three
        'human', 'mouse', 'rat',                

        # primates, alphabetical
        'chimpanzee', 'rhesus macaque',

        # mammals, alphabetical
        'cattle', 'dog',

        # others, alphabetical
        'chicken', 'western clawed frog', 'zebrafish',
        ]

###--- Functions ---###

def organismCompare (a):
        # return a sort key for sorting organisms such that preferred organisms
        # come first (and in the desired order), followed by others

        if a in preferredOrganisms:
                pref = preferredOrganisms.index(a)
        else:
                pref = 9999
        return (pref, a)

###--- Classes ---###

class Cluster:
        def __init__ (self,
                clusterKey, 
                accID, 
                clusterType, 
                source, 
                version, 
                date,
                secondarySource = None
                ):

                self.key = clusterKey
                self.accID = accID
                self.clusterType = clusterType
                self.source = source
                self.version = version
                self.date = date 
                self.secondarySource = secondarySource
                self.organisms = {}     # organism -> [ marker key 1, ... ]
                self.markers = {}       # marker key -> [ symbol, organism,
                                        #    qualifier, refsKey, markerKey ]
                return

        def addRow (self,
                markerKey, 
                symbol,
                seqNum, 
                organism, 
                qualifier, 
                refsKey
                ):

                self.markers[markerKey] = [ symbol, organism, qualifier,
                        refsKey, markerKey ]

                if organism not in self.organisms:
                        self.organisms[organism] = []
                self.organisms[organism].append (markerKey)
                return 

        def getSecondarySource(self):
                return self.secondarySource

        def getClusterData (self):
                return (self.key, self.accID, self.clusterType, self.source,
                        self.version, self.date)

        def getMouseSymbolInCluster (self):
                for markerKey in self.markers.keys():
                        [ symbol, organism, qualifier, refsKey, markerKey ] = self.markers[markerKey]
                        if organism == 'mouse':
                                return symbol
                return None
            
        def getOrganisms (self):
                organisms = list(self.organisms.keys())
                organisms.sort (key=organismCompare)
                return organisms

        def getMarkers (self, organism):
                # collect the markers for this organism and return list of
                # them, each as [ symbol, organism, qualifier, refsKey,
                # markerKey ]

                markers = []
                for markerKey in self.organisms[organism]:
                        markers.append (self.markers[markerKey])

                # sort the markers for this organism

                markers.sort (key=lambda a: symbolsort.splitter(a[0]))
                return markers

class HomologyClusterGatherer (Gatherer.MultiFileGatherer):
        # Is: a data gatherer for the homology_cluster flower
        # Has: queries to execute against the source database
        # Does: queries the source database for primary data for the 
        #       homology cluster flower tables, collates results, writes
        #       tab-delimited text file

        def collateResults (self):

                cols, rows = self.results[0]

                clusterKeyPos = Gatherer.columnNumber (cols, '_Cluster_key')
                typePos = Gatherer.columnNumber (cols, 'clusterType')
                sourcePos = Gatherer.columnNumber (cols, 'source')
                idPos = Gatherer.columnNumber (cols, 'clusterID')
                versionPos = Gatherer.columnNumber (cols, 'version')
                datePos = Gatherer.columnNumber (cols, 'cluster_date')
                markerPos = Gatherer.columnNumber (cols, '_Marker_key')
                symbolPos = Gatherer.columnNumber (cols, 'symbol')
                seqNumPos = Gatherer.columnNumber (cols, 'sequenceNum')
                organismPos = Gatherer.columnNumber (cols, 'organism')
                qualifierPos = Gatherer.columnNumber (cols, 'qualifier')
                refsPos = Gatherer.columnNumber (cols, '_Refs_key')
                secSourcePos = Gatherer.columnNumber (cols, 'secondary_source')

                clusters = {}   # cluster key -> Cluster object

                # Time to build Cluster objects using our data from the
                # database.  Note that no particular ordering of records from
                # the results is assumed.

                for row in rows:
                        clusterKey = row[clusterKeyPos]

                        if clusterKey not in clusters:
                                # We've not yet seen this cluster, so make a
                                # new object.

                                clusterType = row[typePos]
                                source = row[sourcePos]
                                accID = row[idPos]
                                version = row[versionPos]
                                date = row[datePos]

                                # secondary source should be either Alliance Direct or Alliance Clustered
                                secondarySource = row[secSourcePos]

                                if date:
                                        date = str(date)[:10]

                                cluster = Cluster (clusterKey, accID,
                                        clusterType, source, version, date,
                                        secondarySource)

                                clusters[clusterKey] = cluster
                        else:
                                # Look up the cluster we already created.
                                cluster = clusters[clusterKey]

                        markerKey = row[markerPos]
                        symbol = row[symbolPos]
                        seqNum = row[seqNumPos]
                        organism = row[organismPos]
                        qualifier = row[qualifierPos]
                        refsKey = row[refsPos]

                        # cleanup of organism text

                        organism = organism.replace(', laboratory', '')
                        organism = organism.replace(', domestic', '')

                        cluster.addRow (markerKey, symbol, seqNum, organism, 
                                qualifier, refsKey)

                # Our data is all in memory now.  Time to collate and produce
                # our result sets.

                hcCols = [ 'clusterKey', 'clusterID', 'version',
                        'cluster_date', 'source', 'secondary_source', 
                        'hasComparativeGOGraph' ]
                hcRows = []

                hcoCols = [ 'clusterOrganismKey', 'clusterKey', 'organism',
                        'seqNum' ]
                hcoRows = []
                coKey = 0

                hcotmCols = [ 'clusterOrganismKey', '_Marker_key',
                        '_Refs_key', 'qualifier', 'seqNum' ]
                hcotmRows = []

                hccCols = [ 'clusterKey', 'mouseMarkerCount',
                        'humanMarkerCount', 'ratMarkerCount',
                        'cattleMarkerCount', 'chimpMarkerCount',
                        'dogMarkerCount', 'monkeyMarkerCount',
                        'chickenMarkerCount', 'xenopusMarkerCount', 'zebrafishMarkerCount', ]
                hccRows = []

                clusterKeys = list(clusters.keys())
                clusterKeys.sort()

                logger.debug ('%d clusters' % len(clusterKeys))

                secondarySource = None

                for clusterKey in clusterKeys:
                        cluster = clusters[clusterKey]

                        counts = {}

                        (key, accID, clusterType, source, version, date) = \
                                cluster.getClusterData()

                        hcRows.append ( [key, accID, version, date, source,
                                cluster.getSecondarySource(),
                                GOGraphs.hasComparativeGOGraph(cluster.getMouseSymbolInCluster()) ] )
                        coSeqNum = 0
                        hcotmSeqNum = 0

                        for organism in cluster.getOrganisms():
                                coKey = coKey + 1
                                coSeqNum = coSeqNum + 1

                                hcoRows.append ( [coKey, key,
                                        utils.cleanupOrganism(organism),
                                        coSeqNum] )

                                markers = cluster.getMarkers(organism)
                                counts[organism] = len(markers)

                                for [ symbol, organism, qualifier, refsKey,
                                        markerKey ] in markers:

                                        hcotmSeqNum = hcotmSeqNum + 1

                                        hcotmRows.append ( [coKey, markerKey,
                                                refsKey, qualifier,
                                                hcotmSeqNum] )

                        hccRow = [ clusterKey ]
                        for organism in [ 'mouse', 'human', 'rat', 'cattle',
                                        'chimpanzee', 'dog', 'rhesus macaque',
                                        'chicken', 'xenopus', 'zebrafish', ]:
                                if organism in counts:
                                        hccRow.append (counts[organism])
                                else:
                                        hccRow.append (0)
                        hccRows.append (hccRow)

                logger.debug ('%d organisms for clusters' % len(hcoRows))
                logger.debug ('%d markers in clusters' % len(hcotmRows))

                self.output.append ( (hcCols, hcRows) )
                self.output.append ( (hcoCols, hcoRows) )
                self.output.append ( (hcotmCols, hcotmRows) )
                self.output.append ( (hccCols, hccRows) ) 
                return

###--- globals ---###

cmds = [
        '''select mc._Cluster_key,
                typ.term as clusterType,
                src.abbreviation as source,
                mc.clusterID,
                mc.version,
                mc.cluster_date,
                mcm._Marker_key,
                mm.symbol,
                mcm.sequenceNum,
                mo.commonName as organism,
                null as qualifier,
                null as _Refs_key,
                src.abbreviation as secondary_source
        from mrk_cluster mc
        inner join mrk_clustermember mcm on (mc._Cluster_key = mcm._Cluster_key)
        inner join mrk_marker mm on (mcm._Marker_key = mm._Marker_key)
        inner join voc_term typ on (mc._ClusterType_key = typ._Term_key)
        inner join mgi_organism mo on (mm._Organism_key = mo._Organism_key)
        inner join voc_term src on (mc._ClusterSource_key = src._Term_key)
        where mc._ClusterSource_key in (%d, %d)
                and mc._ClusterType_key = %d''' % (
                        ALLIANCE_DIRECT, ALLIANCE_CLUSTERED, HOMOLOGY),
        ]

# data about files to be written; for each:  (filename prefix, list of field
#       names in order to be written, name of table to be loaded)
files = [
        ('homology_cluster',
                [ 'clusterKey', 'clusterID', 'version', 'cluster_date',
                        'source', 'secondary_source',
                        'hasComparativeGOGraph', ],
                'homology_cluster'),

        ('homology_cluster_organism',
                [ 'clusterOrganismKey', 'clusterKey', 'organism', 'seqNum' ],
                'homology_cluster_organism'),

        ('homology_cluster_organism_to_marker',
                [ Gatherer.AUTO, 'clusterOrganismKey', '_Marker_key',
                        '_Refs_key', 'qualifier', 'seqNum' ],
                'homology_cluster_organism_to_marker'),

        ('homology_cluster_counts',
                [ 'clusterKey', 'mouseMarkerCount', 'humanMarkerCount',
                        'ratMarkerCount', 'cattleMarkerCount',
                        'chimpMarkerCount', 'dogMarkerCount', 
                        'monkeyMarkerCount', 'chickenMarkerCount',
                        'xenopusMarkerCount', 'zebrafishMarkerCount', ],
                'homology_cluster_counts'),
        ]

# global instance of a HomologyClusterGatherer
gatherer = HomologyClusterGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
