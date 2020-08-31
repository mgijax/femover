#!./python
# 
# gathers data for the 'probe_id' table in the front-end database

import Gatherer

###--- Classes ---###

ProbeIDGatherer = Gatherer.Gatherer
        # Is: a data gatherer for the probe_id table
        # Has: queries to execute against the source database
        # Does: queries the source database for primary data for probe IDs,
        #       collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
        # 0. primary and secondary IDs for probes -- BUT exclude sequence IDs, as we already
        #       have those relationships in the probe_to_sequence table and we don't show the
        #       sequence IDs anywhere on the probe detail page currently.
        '''select a._Object_key as probeKey, a._LogicalDB_key,
                a.accID, a.preferred, a.private, ldb.name as logicalDB
        from acc_accession a, acc_logicaldb ldb
        where a._MGIType_key = 3
                and a._LogicalDB_key = ldb._LogicalDB_key
                and ldb._LogicalDB_key not in (9, 25)
                and exists (select 1 from prb_probe aa
                        where a._Object_key = aa._probe_key)'''
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'probeKey', 'logicalDB', 'accID', 'preferred', 'private' ]

# prefix for the filename of the output file
filenamePrefix = 'probe_id'

# global instance of a ProbeIDGatherer
gatherer = ProbeIDGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
