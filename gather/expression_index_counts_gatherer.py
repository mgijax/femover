#!./python
# 
# gathers data for the 'expression_index_counts' table in the front-end db

import Gatherer
import logger

###--- Globals ---###

ASSAY_AGE_COUNT = 'assay_age_count'
FC_ASSAY_COUNT = 'fully_coded_assay_count'
FC_RESULT_COUNT = 'fully_coded_result_count'

###--- Classes ---###

class IndexCountsGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the expression_index_counts table
        # Has: queries to execute against the source database
        # Does: queries the source database for various counts for GXD
        #       Literature Index entries, collates results, writes
        #       tab-delimited text file

        def collateResults (self):
                # collate the results of our queries to produce an integrated
                # set of final results for the table

                redBallCounts = {}
                fcAssayCounts = {}
                fcResultCounts = {}

                # each set of results has a distinct set of index keys and
                # an associated 'myCount' for each.

                items = [ (0, redBallCounts, 'full set'),
                        (1, fcAssayCounts, 'fc assays'),
                        (2, fcResultCounts, 'fc results') ]

                for (query, resultDict, description) in items:
                        cols, rows = self.results[query]

                        keyCol = Gatherer.columnNumber (cols, '_Index_key')
                        countCol = Gatherer.columnNumber (cols, 'myCount')

                        for row in rows:
                                resultDict[row[keyCol]] = row[countCol]
                        logger.debug ('Found %d entries for %s' % (
                                len(resultDict), description) ) 

                # now consolidate into a single list of results; all index
                # entries will appear in redBallCounts.

                self.finalColumns = [ '_Index_key', ASSAY_AGE_COUNT,
                        FC_ASSAY_COUNT, FC_RESULT_COUNT ]
                self.finalResults = []

                indexKeys = list(redBallCounts.keys())
                indexKeys.sort()

                for key in indexKeys:
                        assayCount = 0
                        resultCount = 0
                        redBallCount = redBallCounts[key]

                        if key in fcAssayCounts:
                                assayCount = fcAssayCounts[key]
                        if key in fcResultCounts:
                                resultCount = fcResultCounts[key]

                        self.finalResults.append ( [key, redBallCount,
                                assayCount, resultCount] )
                return

###--- globals ---###

cmds = [
        # 0. number of red balls (assay type, age pairs) for each index entry
        '''select i._Index_key,
                count(1) as myCount
        from gxd_index i,
                gxd_index_stages s
        where i._Index_key = s._Index_key
        group by i._Index_key''',

        # 1. number of fully-coded assays for each index entry
        '''select i._Index_key,
                count(distinct ge._Assay_key) as myCount
        from gxd_index i,
                gxd_assay ga,
                gxd_expression ge
        where i._Refs_key = ga._Refs_key
                and i._Marker_key = ga._Marker_key
                and ga._Assay_key = ge._Assay_key
                and ge.isForGXD = 1
                and ga._Marker_key = ge._Marker_key
        group by i._Index_key''',

        # 2. number of fully-coded results for each index entry
        '''select i._Index_key,
                count(distinct ge._Expression_key) as myCount
        from gxd_index i,
                gxd_assay ga,
                gxd_expression ge
        where i._Refs_key = ga._Refs_key
                and i._Marker_key = ga._Marker_key
                and ga._Assay_key = ge._Assay_key
                and ge.isForGXD = 1
                and ga._Marker_key = ge._Marker_key
        group by i._Index_key''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        '_Index_key', ASSAY_AGE_COUNT, FC_ASSAY_COUNT, FC_RESULT_COUNT
        ]

# prefix for the filename of the output file
filenamePrefix = 'expression_index_counts'

# global instance of a IndexCountsGatherer
gatherer = IndexCountsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
