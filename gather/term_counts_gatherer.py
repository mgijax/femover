#!./python
# 
# gathers data for the 'term_counts' table in the front-end database

import Gatherer
import logger
import TermCounts

###--- Classes ---###

class TermCountsGatherer (Gatherer.ChunkGatherer):
        # Is: a data gatherer for the term_counts table
        # Has: queries to execute against the source database
        # Does: queries the source database for counts related to vocab terms,
        #       collates results, writes tab-delimited text file

        def getMinKeyQuery(self):
                return 'select min(_Vocab_key) from voc_vocab'
        
        def getMaxKeyQuery(self):
                return 'select max(_Vocab_key) from voc_vocab'
        
        def postprocessResults(self):
                # gather and add any extra counts

                self.convertFinalResultsToList()

                self.finalColumns = self.finalColumns + \
                        [ 'markerCount', 'expressionMarkerCount','creMarkerCount', 'gxdLitMarkerCount' ]

                termKeyCol = Gatherer.columnNumber(self.finalColumns, '_Term_key')
                
                for row in self.finalResults:
                        key = row[termKeyCol]
                        row.append (TermCounts.getMarkerCount(key))
                        row.append (TermCounts.getExpressionMarkerCount(key))
                        row.append (TermCounts.getCreMarkerCount(key))
                        row.append (TermCounts.getLitIndexMarkerCount(key))

                TermCounts.reset()
                return

###--- globals ---###

cmds = [
        '''with selected_vocabs as (select _Vocab_key
                        from voc_vocab
                        where _Vocab_key >= %d and _Vocab_key < %d
                ),
                children as (select p._Object_key as _Term_key, count(distinct de._Child_key) as childCount
                        from dag_node p, dag_edge de
                        where p._Node_key = de._Parent_key
                        group by p._Object_key
                )
                select t._Term_key,
                        case
                                when c.childCount is null then 0
                                else c.childCount
                        end as childCount
                from voc_term t
                inner join selected_vocabs vv on (t._Vocab_key = vv._Vocab_key)
                left outer join children c on (t._Term_key = c._Term_key)'''
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Term_key', 'childCount', 'markerCount', 'expressionMarkerCount','creMarkerCount', 'gxdLitMarkerCount' ]

# prefix for the filename of the output file
filenamePrefix = 'term_counts'

# global instance of a TermCountsGatherer
gatherer = TermCountsGatherer (filenamePrefix, fieldOrder, cmds)

# must process one vocab at a time for the TermCounts library
gatherer.setChunkSize(1)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
