#!./python
# 
# gathers data for the 'expression_index_sequence_num' table in the front-end
# database

import Gatherer
import symbolsort
import logger

###--- Globals ---###

REFERENCE = 'byReference'
MARKER = 'byMarker'

###--- Classes ---###

class ExpressionIndexSequenceNumGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the expression_index_sequence_num table
        # Has: queries to execute against the source database
        # Does: queries the source database for ordering data for GXD
        #       literature index records, collates results, writes
        #       tab-delimited text file

        def collateResults (self):

                # collect and sort references

                columns, rows = self.results[0]

                refCol = Gatherer.columnNumber (columns, '_Refs_key')
                yearCol = Gatherer.columnNumber (columns, 'year')
                accCol = Gatherer.columnNumber (columns, 'numericPart')

                toSort = []

                for r in rows:
                        toSort.append ( (r[yearCol], r[accCol], r[refCol]) )
                toSort.sort()

                i = 0
                refs = {}
                for (year, jnum, key) in toSort:
                        i = i + 1
                        refs[key] = i

                logger.debug ('Sorted %d references' % len(refs))

                # collect and sort markers

                columns, rows = self.results[1]

                mrkCol = Gatherer.columnNumber (columns, '_Marker_key')
                symCol = Gatherer.columnNumber (columns, 'symbol')

                toSort = []

                for r in rows:
                        toSort.append ( (r[symCol], r[mrkCol]) )
                toSort.sort(key=lambda a : symbolsort.splitter(a[0]))

                i = 0
                markers = {}
                for (symbol, key) in toSort:
                        i = i + 1
                        markers[key] = i

                logger.debug ('Sorted %d markers' % len(markers))

                # pull together the list by index key

                self.finalColumns = [ '_Index_key', 'referenceSeqNum',
                        'markerSeqNum' ]
                self.finalResults = []

                columns, rows = self.results[2]

                indexCol = Gatherer.columnNumber (columns, '_Index_key')
                mrkCol = Gatherer.columnNumber (columns, '_Marker_key')
                refCol = Gatherer.columnNumber (columns, '_Refs_key')

                for r in rows:
                        self.finalResults.append ( (r[indexCol],
                                refs[r[refCol]], markers[r[mrkCol]]) ) 
                return

###--- globals ---###

cmds = [
        '''select distinct i._Refs_key, r.year, c.numericPart
                from bib_refs r, bib_citation_cache c, gxd_index i
                where r._Refs_key = c._Refs_key
                        and r._Refs_key = i._Refs_key''',

        '''select distinct i._Marker_key, m.symbol
                from gxd_index i, mrk_marker m
                where i._Marker_key = m._Marker_key''',

        'select _Index_key, _Marker_key, _Refs_key from gxd_index',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Index_key', 'referenceSeqNum', 'markerSeqNum' ]

# prefix for the filename of the output file
filenamePrefix = 'expression_index_sequence_num'

# global instance of a ExpressionIndexSequenceNumGatherer
gatherer = ExpressionIndexSequenceNumGatherer (filenamePrefix, fieldOrder,
        cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
