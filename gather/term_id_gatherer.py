#!./python
# 
# gathers data for the 'term_id' table in the front-end database

import Gatherer

###--- Classes ---###

class TermIDGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the term_id table
        # Has: queries to execute against the source database
        # Does: queries the source database for primary data for term IDs,
        #       collates results, writes tab-delimited text file
        pass

###--- globals ---###

cmds = [
        '''select a._Object_key as termKey, a._LogicalDB_key,
                a.accID, a.preferred, a.private, ldb.name as logicalDB
        from acc_accession a, acc_logicaldb ldb
        where a._MGIType_key = 13
                and a._LogicalDB_key = ldb._LogicalDB_key'''
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'termKey', 'logicalDB', 'accID', 'preferred',
        'private' ]

# prefix for the filename of the output file
filenamePrefix = 'term_id'

# global instance of a TermIDGatherer
gatherer = TermIDGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
