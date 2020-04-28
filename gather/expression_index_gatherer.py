#!./python
# 
# gathers data for the 'expression_index' table in the front-end database

import Gatherer

###--- Classes ---###

ExpressionIndexGatherer = Gatherer.Gatherer
        # Is: a data gatherer for the expression_index table
        # Has: queries to execute against the source database
        # Does: queries the source database for the GXD literature index,
        #       collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
        '''select i._Index_key, i._Refs_key, c.jnumID, m._Marker_key,
                        m.symbol, m.name, a.accID, i.comments
                from gxd_index i, bib_citation_cache c, mrk_marker m,
                        acc_accession a
                where i._Marker_key = m._Marker_key
                        and m._Marker_key = a._Object_key
                        and a._MGIType_key = 2
                        and a._LogicalDB_key = 1
                        and a.preferred = 1
                        and a.private = 0
                        and i._Refs_key = c._Refs_key''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        '_Index_key', '_Refs_key', 'jnumID', '_Marker_key', 'symbol',
        'name', 'accID', 'comments'
        ]

# prefix for the filename of the output file
filenamePrefix = 'expression_index'

# global instance of a ExpressionIndexGatherer
gatherer = ExpressionIndexGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
