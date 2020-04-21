#!./python
# 
# gathers data for the 'batch_marker_alleles' table in the front-end database

import Gatherer

###--- Classes ---###

class BatchMarkerAllelesGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the batch_marker_alleles table
        # Has: queries to execute against the source database
        # Does: queries the source database for a tiny subset of allele data
        #       that we need for batch query results that include alleles,
        #       collates results, writes tab-delimited text file

        def postprocessResults (self):
                self.convertFinalResultsToList()

                i = 1
                for row in self.finalResults:
                        self.addColumn ('sequence_num', i, row,
                                self.finalColumns)
                        i = i + 1
                return

###--- globals ---###

cmds = [
        '''select a._Marker_key, a.symbol, ac.accid, ac.numericPart
                from all_allele a,
                        acc_accession ac
                where a._Allele_key = ac._Object_key
                        and ac._MGIType_key = 11
                        and ac.preferred = 1
                        and ac._LogicalDB_key = 1
                        and ac.private = 0
                        and a.isWildType = 0
                        and a._Marker_key is not null
                order by a._Marker_key, ac.numericPart''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        Gatherer.AUTO, '_Marker_key', 'symbol', 'accid', 'sequence_num'
        ]

# prefix for the filename of the output file
filenamePrefix = 'batch_marker_alleles'

# global instance of a BatchMarkerAllelesGatherer
gatherer = BatchMarkerAllelesGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
