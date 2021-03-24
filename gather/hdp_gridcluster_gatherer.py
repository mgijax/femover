#!./python
# 
# gathers data for the hdp_gridluster* tables in the front-end database

import Gatherer
import DiseasePortalUtils
import HomologyUtils
import MarkerUtils
import logger

###--- Globals ---###

DPU = DiseasePortalUtils        # module alias for convenience
HU = HomologyUtils              # module alias for convenience
MU = MarkerUtils                # module alias for convenience

###--- Classes ---###

class GridClusterGatherer (Gatherer.MultiFileGatherer):
        # Is: a data gatherer for the gridclulster* tables
        # Has: queries to execute against the source database
        # Does: queries the source database for data to pull into the 
        #       gridcluster* tables, collates results, writes tab-delimited
        #       text file

        def collateResults(self):
                # highest cluster key from the source database
                maxClusterKey = HU.getMaxClusterKey()

                # walk through the marker / cluster data

                cols, rows = DPU.getClusteredMarkers()

                clusterKeyCol = Gatherer.columnNumber (cols, '_Cluster_key')
                markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')

                # set of distinct clusters
                clusterList = set([])

                # at most one marker in a gridCluster_marker
                markerList = set([])

                clusterResults = []     # rows for hdp_gridcluster
                cmarkerResults = []     # rows for hdp_gridcluster_marker

                for row in rows:
                        hybridClusterKey = row[clusterKeyCol]
                        markerKey = row[markerKeyCol]

                        # if multiples, just take the last cluster key.
                        srcKeys = HU.getAllianceDirectClusterKeys(markerKey)

                        srcClusterKey = srcKeys[-1]
                        homologeneID = None
                        source = HU.getSource(srcClusterKey)

                        if srcClusterKey not in clusterList:
                                clusterResults.append( [ 
                                        srcClusterKey,
                                        homologeneID,
                                        source,
                                        ])
                                clusterList.add(srcClusterKey)

                        if markerKey not in markerList:
                                cmarkerResults.append ( [ 
                                        srcClusterKey,
                                        markerKey,
                                        MU.getOrganismKey(markerKey),
                                        MU.getSymbol(markerKey)
                                        ])
                                markerList.add(markerKey)

                logger.debug ('processed %d markers in %d clusters' % (
                        len(clusterResults), len(cmarkerResults)) )
                
                # add other markers to their respective clusters, if those
                # markers didn't have their own annotations

                byCluster = HU.getMarkersPerCluster(HU.ALLIANCE_DIRECT)
                addedCount = 0

                for clusterKey in list(byCluster.keys()):

                        # assume no markers in the cluster had annotations
                        clusterHadData = False

                        for markerKey in byCluster[clusterKey]:
                                if markerKey in markerList:
                                        clusterHadData = True
                                        break

                        # if any markers had data, then we need to toss any
                        # missing markers into the gridcluster

                        if clusterHadData:
                                for markerKey in byCluster[clusterKey]:
                                        if markerKey not in markerList:
                                                cmarkerResults.append ( [
                                                        clusterKey,
                                                        markerKey,
                                                        MU.getOrganismKey(markerKey),
                                                        MU.getSymbol(markerKey),
                                                        ] )
                                                markerList.add(markerKey) 
                                                addedCount = addedCount + 1

                logger.debug('added %d markers with no annotations' % \
                        addedCount)

                # process mouse/human markers with annotations that are NOT 
                # part of homology clusters

                (cols, rows) = DPU.getNonClusteredMarkers()

                # set of columns for common sql fields
                markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')

                # We're going to be generating fake "cluster keys" to shoehorn
                # the data in, so start after the current max.
                clusterKey = maxClusterKey

                ct = 0          # number of fake clusters added

                for row in rows:
                        markerKey = row[markerKeyCol]

                        # to make it repeatable, just add the marker key to
                        # the maximum cluster key
                        clusterKey = maxClusterKey + markerKey

                        clusterResults.append ( [ 
                                clusterKey,
                                None,
                                None,
                                ])

                        if markerKey not in markerList:
                                cmarkerResults.append ( [ 
                                        clusterKey,
                                        markerKey,
                                        MU.getOrganismKey(markerKey),
                                        MU.getSymbol(markerKey)
                                ])
                                markerList.add(markerKey)

                        ct = ct + 1

                logger.debug('Added %d fake clusters' % ct)

                hgCols = [ '_Cluster_key', 'homologene_id', 'source' ]
                hgmCols = [ 'hdp_gridcluster_key', 'marker_key',
                        'organism_key', 'symbol' ]

                self.output.append( (hgCols, clusterResults) )
                self.output.append( (hgmCols, cmarkerResults) )
                return

###--- globals ---###

cmds = [
        # 0. all data pulled from libraries, so just have a no-op here
        'select 1',
        ]

files = [
        ('hdp_gridcluster',
                [ '_Cluster_key', 'homologene_id', 'source' ],
                'hdp_gridcluster'),

        ('hdp_gridcluster_marker',
                [ Gatherer.AUTO, 'hdp_gridcluster_key', 'marker_key',
                        'organism_key', 'symbol' ],
                'hdp_gridcluster_marker'),
        ]

# global instance of a GridClusterGatherer
gatherer = GridClusterGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
