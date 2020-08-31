#!./python
# 
# gathers data for the 'probe_alias' table in the front-end database

import Gatherer
import symbolsort
import logger

###--- Globals ---###

aliasCol = None
keyCol = None

###--- Classes ---###

class ProbeAliasGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the probe_alias table
        # Has: queries to execute against the source database
        # Does: queries the source database for aliases for probes,
        #       collates results, writes tab-delimited text file

        def postprocessResults(self):
                global aliasCol, keyCol

                self.convertFinalResultsToList()
                
                aliasCol = Gatherer.columnNumber(self.finalColumns, 'alias')
                keyCol = Gatherer.columnNumber(self.finalColumns, '_Reference_key')
                
                self.finalResults.sort(key=lambda x: (x[keyCol], symbolsort.splitter(x[aliasCol])))
                logger.debug('Sorted %d aliases' % len(self.finalResults))
                
                self.finalColumns.append('sequence_num')
                i = 0
                for row in self.finalResults:
                        i = i + 1
                        row.append(i)
                        
                logger.debug('Assigned %d sequence numbers' % len(self.finalResults))
                return

###--- globals ---###

cmds = [ 'select _Reference_key, alias from prb_alias' ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        Gatherer.AUTO, '_Reference_key', 'alias', 'sequence_num',
        ]

# prefix for the filename of the output file
filenamePrefix = 'probe_alias'

# global instance of a ProbeAliasGatherer
gatherer = ProbeAliasGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
