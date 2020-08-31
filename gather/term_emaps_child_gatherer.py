#!./python
# 
# gathers data for the 'term_emaps_child' table in the front-end database

import Gatherer

###--- Classes ---###

TermEmapsChildGatherer = Gatherer.Gatherer
        # Is: a data gatherer for the term_emaps_child table
        # Has: queries to execute against the source database
        # Does: queries the source database for relationships between EMAPA
        #       terms and the EMAPS terms that derive from it, collates
        #       results, writes tab-delimited text file

###--- globals ---###

cmds = [ '''select _Emapa_term_key, _Term_key as _Emaps_term_key
        from voc_term_emaps''', 
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        Gatherer.AUTO, '_Emapa_term_key', '_Emaps_term_key'
        ]

# prefix for the filename of the output file
filenamePrefix = 'term_emaps_child'

# global instance of a TermEmapsChildGatherer
gatherer = TermEmapsChildGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
