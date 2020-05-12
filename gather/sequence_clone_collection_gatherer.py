#!./python
# 
# gathers data for the 'sequenceCloneCollection' table in the front-end
# database

import Gatherer

###--- Classes ---###

SequenceCloneCollectionGatherer = Gatherer.Gatherer
        # Is: a data gatherer for the sequenceCloneCollection table
        # Has: queries to execute against the source database
        # Does: queries the source database for clone collections for
        #       sequences, collates results, writes tab-delimited text file

###--- globals ---###

cmds = [ '''select distinct spc._Sequence_key, ldb.name as collection
        from seq_probe_cache spc,
                mgi_setmember msm,
                mgi_set ms,
                acc_logicaldb ldb,
                acc_accession aa
        where spc._Probe_key = aa._Object_key
                and aa._MGIType_key = 3
                and aa._LogicalDB_key = ldb._LogicalDB_key
                and aa._LogicalDB_key = msm._Object_key
                and msm._Set_key = ms._Set_key
                and ms.name = 'Clone Collection (all)' '''
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Sequence_key', 'collection', ]

# prefix for the filename of the output file
filenamePrefix = 'sequence_clone_collection'

# global instance of a SequenceCloneCollectionGatherer
gatherer = SequenceCloneCollectionGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
