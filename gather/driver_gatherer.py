#!./python
# 
# gathers data for the 'driver' table in the front-end database

import Gatherer

ALLELE_TO_DRIVER_CAT_KEY = 1006

###--- Classes ---###

class DriverGatherer (Gatherer.Gatherer) :
        # Is: a data gatherer for the driver table
        # Has: queries to execute against the source database
        # Does: queries the source database for recombinase driver data 

        def collateResults(self):

            lcSymbol2cluster = {}
            group1Keys = set()
            markerCounts = {}

            # First query returns Alliance drivers. 
            # Create some lookups from the results. Then initialize the
            # final results to these results
            self.finalColumns = ['driver_key', 'driver', 'cluster_key']
            self.finalResults = []

            cols0,rows0 = self.results[0]
            maxClusterKey = -1
            for r in rows0:
                markerKey = r[0]
                markerCounts[markerKey] = markerCounts.get(markerKey,0) + 1

            for r in rows0:
                markerKey = r[0]
                if markerCounts[markerKey] != 1:
                    continue
                symbol = r[1]
                organism = r[2]
                clusterKey = r[3]
                group1Keys.add(markerKey)
                maxClusterKey = max(maxClusterKey, clusterKey)
                lcSymbol2cluster[symbol.lower()] = clusterKey
                self.finalResults.append( (markerKey, symbol, clusterKey) )


            cols1,rows1 = self.results[1]
            for r in rows1:
                markerKey = r[0]
                symbol = r[1]
                organismKey = r[2]
                if markerKey in group1Keys:
                    continue
                clusterKey = lcSymbol2cluster.get(symbol.lower(), None)
                if clusterKey is None:
                    maxClusterKey += 1
                    clusterKey = maxClusterKey
                self.finalResults.append( (markerKey, symbol, clusterKey) )

            self.finalResults.sort(key = lambda r: r[2])

###--- globals ---###

cmds = [
        # Query 0: returns all markers that are used as drivers and are members of an Alliance Direct homology cluster.
        # Includes the cluster key with each marker returned.
        '''
            select distinct
                m._marker_key, m.symbol, m._organism_key, mc._cluster_key
            from MRK_Cluster mc 
            join MRK_ClusterMember mcm on mcm._cluster_key = mc._cluster_key
            join MRK_Marker m on m._marker_key = mcm._marker_key   
            join VOC_Term clustertype on clustertype._term_key = mc._clustertype_key 
                and clustertype.term = 'homology'
            join VOC_Term source on source._term_key = mc._clustersource_key 
                and source.term = 'Alliance' 
                and source.abbreviation = 'Alliance Direct'
            join MGI_Relationship r on r._object_key_2 = m._marker_key and r._category_key = 1006
	    join VOC_Annot va on r._object_key_1 = va._object_key and va._annottype_key = 1014
	    join VOC_Term vt on va._term_key = vt._term_key and vt.term = 'Recombinase'

            order by mc._cluster_key
        ''',
        # Query 1: returns all markers used as drivers. For anything not seen in the first group,
        # use try to assign a cluster by matching on symbol (case insensitive). If that fails, assign its
        # own unique cluster key
        '''
            select distinct
                m._marker_key, m.symbol, m._organism_key
            from MRK_Marker m
            join MGI_Relationship r on r._object_key_2 = m._marker_key and r._category_key = 1006
	    join VOC_Annot va on r._object_key_1 = va._object_key and va._annottype_key = 1014
	    join VOC_Term vt on va._term_key = vt._term_key and vt.term = 'Recombinase'
        ''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ 'driver_key', 'driver', 'cluster_key' ]

# prefix for the filename of the output file
filenamePrefix = 'driver'

# global instance of a DriverGatherer
gatherer = DriverGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
