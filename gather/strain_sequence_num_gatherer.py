#!./python
# 
# gathers data for the 'strain_sequence_num' table in the front-end database

import Gatherer
import symbolsort
import logger
import StrainUtils

###--- Globals ---###

keyCol = None
strainCol = None

###--- Functions ---###

def rowSortKey(a):
        # get a key for sorting rows:  first by strain name, then secondarily by key
        return (symbolsort.splitter(a[strainCol]), a[keyCol])

###--- Classes ---###

class StrainSequenceNumGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the strain_sequence_num table
        # Has: queries to execute against the source database
        # Does: queries the source database for ordering data for strains,
        #       collates results, writes tab-delimited text file
        
        def collateResults(self):
                global keyCol, strainCol

                cols, rows = self.results[0]
                keyCol = Gatherer.columnNumber(cols, '_Strain_key')
                strainCol = Gatherer.columnNumber(cols, 'strain')

                rows.sort(key=rowSortKey)
                logger.debug('Sorted %d rows' % len(rows))

                        
                self.finalColumns = cols
                self.finalColumns.append('by_strain')

                self.finalResults = rows
                self.convertFinalResultsToList()
                i = 0
                for row in self.finalResults:
                        i = i + 1
                        row.append(i)
                logger.debug('Assigned sequence numbers')
                return

###--- globals ---###

cmds = [
        # 0. strain keys and names for ordering
        '''select s._Strain_key, s.strain
                from prb_strain s, %s t
                where s._Strain_key = t._Strain_key''' % StrainUtils.getStrainTempTable(),
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Strain_key', 'by_strain', ]

# prefix for the filename of the output file
filenamePrefix = 'strain_sequence_num'

# global instance of a StrainSequenceNumGatherer
gatherer = StrainSequenceNumGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
