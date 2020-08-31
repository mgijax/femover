#!./python
# 
# gathers data for the 'term_sequence_num' table in the front-end database

import Gatherer
import logger
import VocabSorter

###--- Classes ---###

class TermSequenceNumGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the term_sequence_num table
        # Has: queries to execute against the source database
        # Does: queries the source database for ordering data for terms,
        #       collates results, writes tab-delimited text file

        def collateResults (self):
                cols, rows = self.results[0]

                keyCol = Gatherer.columnNumber (cols, '_Term_key')
                termCol = Gatherer.columnNumber (cols, 'term')
                seqNumCol = Gatherer.columnNumber (cols, 'sequenceNum')

                self.finalColumns = [ 'term_key', 'by_default', 'by_dfs',
                        'by_vocab_dag_alpha', ]
                self.finalResults = []

                toSort = []
                default = {}

                for row in rows:
                        if row[termCol]:
                                toSort.append ( (row[termCol].lower(),
                                        row[keyCol]) )
                        else:
                                # need a value that will sort to the beginning (space is near the ASCII end)
                                toSort.append ( (' ', row[keyCol]) )
                                # need a value that will sort to the end (tilde is near the ASCII end)
                                #toSort.append ( ('~', row[keyCol]) )

                        if row[seqNumCol]:
                                default[row[keyCol]] = row[seqNumCol]
                toSort.sort()

                logger.debug ('Got %d terms, %d with seq num' % (len(toSort),
                        len(default)) )

                i = 0
                for (term, key) in toSort:
                        i = i + 1
                        if key not in default:
                                default[key] = i

                logger.debug ('Assigned default seq nums')

                defaultKeys = list(default.keys())
                defaultKeys.sort()
                for key in defaultKeys:
                        self.finalResults.append ( [ key, default[key],
                                VocabSorter.getSequenceNum(key),
                                VocabSorter.getVocabDagTermSequenceNum(key) ]
                                )

                logger.debug ('Compiled rows for %d terms' % \
                        len(self.finalResults))

                logger.debug ('Returned %d total rows' % \
                        len(self.finalResults))
                return

###--- globals ---###

cmds = [
        # 0. basic term data
        '''select t._Term_key, t.term, t.sequenceNum
        from voc_term t''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ 'term_key', 'by_default', 'by_dfs', 'by_vocab_dag_alpha', ]

# prefix for the filename of the output file
filenamePrefix = 'term_sequence_num'

# global instance of a TermSequenceNumGatherer
gatherer = TermSequenceNumGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
