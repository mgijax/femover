#!./python
# 
# gathers data for the 'strain_id' table in the front-end database

import Gatherer
import StrainUtils

###--- Classes ---###

StrainIDGatherer = Gatherer.Gatherer
        # Is: a data gatherer for the strain_id table
        # Has: queries to execute against the source database
        # Does: queries the source database for accession IDs for strains,
        #       collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
        # 0. accession IDs for strains.  Order so MGI IDs come first, preferred IDs before secondary.
        #       Suppress FESA IDs for now, as we have nowhere to link them to.
        '''select a._Object_key, a._LogicalDB_key, ldb.name, a.preferred, a.accID, a.preferred, a.private,
                row_number() over (order by a._Object_key, a._LogicalDB_key, a.preferred desc, a.accID) as sequence_num
        from acc_accession a, acc_logicaldb ldb, %s t
        where a._MGIType_key = 10
                and a._LogicalDB_key = ldb._LogicalDB_key
                and a._Object_key = t._Strain_key
        order by a._Object_key, a._LogicalDB_key, a.preferred desc, a.accID''' % StrainUtils.getStrainTempTable(),
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Object_key', 'name', 'accID', 'preferred', 'private', 'sequence_num' ]

# prefix for the filename of the output file
filenamePrefix = 'strain_id'

# global instance of a StrainIDGatherer
gatherer = StrainIDGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
