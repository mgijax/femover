#!./python
# 
# gathers data for the 'marker_microarray' table in the front-end database

import Gatherer
import symbolsort
import logger

###--- Classes ---###

MarkerMicroarrayGatherer = Gatherer.Gatherer

###--- globals ---###

cmds = [
        '''select a._Object_key as marker_key,
                a.accID as probeset_id,
                replace(ldb.name, ' ', '_') || '_mgi.rpt' as report_name,
                ldb.description as platform
        from acc_accession a, acc_logicaldb ldb
        where a._MGIType_key = 2
                and a.private = 0
                and a._LogicalDB_key in (select sm._Object_key
                        from mgi_setmember sm, mgi_set s
                        where sm._Set_key = s._Set_key
                                and s.name = 'MA Chip')
                and a._LogicalDB_key = ldb._LogicalDB_key''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        Gatherer.AUTO, 'marker_key', 'probeset_id', 'platform', 'report_name'
        ]

# prefix for the filename of the output file
filenamePrefix = 'marker_microarray'

# global instance of a MarkerMicroarrayGatherer
gatherer = MarkerMicroarrayGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
