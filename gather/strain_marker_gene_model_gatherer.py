#!./python
# 
# gathers data for the 'strain_marker_gene_model' table in the front-end database

import Gatherer

###--- Globals ---###

###--- Classes ---###

StrainMarkerGeneModelGatherer = Gatherer.Gatherer
        # Is: a data gatherer for the strain_marker table
        # Has: queries to execute against the source database
        # Does: queries the source database for primary data for strain markers,
        #       collates results, writes tab-delimited text file
        
###--- globals ---###

cmds = [
        # 0. pull gene model sequence data from the database
        '''select msm._StrainMarker_key, a_seq.accID, ldb.name as logical_db,
                        row_number() over (order by msm._StrainMarker_key, a_seq.accID) as sequence_num
                from mrk_strainmarker msm
                inner join acc_accession a on (msm._StrainMarker_key = a._Object_key and a._MGIType_key = 44)
                inner join acc_accession a_seq on (a.accID = a_seq.accID and a_seq._MGIType_key = 19)
                inner join acc_logicaldb ldb on (a_seq._LogicalDB_key = ldb._LogicalDB_key)
                order by 4''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_StrainMarker_key', 'accID', 'logical_db', 'sequence_num', ]

# prefix for the filename of the output file
filenamePrefix = 'strain_marker_gene_model'

# global instance of a StrainMarkerGeneModelGatherer
gatherer = StrainMarkerGeneModelGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
