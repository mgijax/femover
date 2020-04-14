#!./python
# 
# gathers data for the 'alleleID' table in the front-end database

import Gatherer

###--- Classes ---###

AlleleIDGatherer = Gatherer.Gatherer
        # Is: a data gatherer for the alleleID table
        # Has: queries to execute against the source database
        # Does: queries the source database for primary data for allele IDs,
        #       collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
        '''select a._Object_key as alleleKey, a._LogicalDB_key,
                a.accID, a.preferred, a.private, ldb.name as logicalDB
        from acc_accession a, acc_logicaldb ldb
        where a._MGIType_key = 11
                and a._LogicalDB_key = ldb._LogicalDB_key
                and exists (select 1 from all_allele aa
                        where a._Object_key = aa._Allele_key)'''
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'alleleKey', 'logicalDB', 'accID', 'preferred',
        'private' ]

# prefix for the filename of the output file
filenamePrefix = 'allele_id'

# global instance of a alleleIDGatherer
gatherer = AlleleIDGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
