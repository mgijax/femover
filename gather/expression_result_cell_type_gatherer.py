#!./python
# 
# gathers data for the 'expression_result_cell_type' table in the front-end database

import Gatherer
import GXDUniUtils
import logger

CLASSICAL_KEY_TABLE = GXDUniUtils.getClassicalKeyTable()

###--- Classes ---###

ExpressionResultCellTypeGatherer = Gatherer.Gatherer
        # Is: a data gatherer for the expression_result_cell_type table
        # Has: queries to execute against the source database
        # Does: queries the source database for cell types connected to in situ expression results,
        #       collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
    '''select ck._Assigned_key, vt.term, a.accid,
            row_number() over (order by ck._Assigned_key, vt.term) as sequenceNum
        from %s ck, gxd_isresultcelltype ct, voc_term vt, acc_accession a
        where ck._Result_key = ct._Result_key
            and ck._CellType_Term_key = ct._CelLType_Term_key
            and ct._CellType_Term_key = vt._Term_key
            and vt._Term_key = a._object_key
            and a._logicaldb_key = 173
            and a._mgitype_key = 13
            and a.preferred = 1
        order by 1, 2''' % CLASSICAL_KEY_TABLE,
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Assigned_key', 'term', 'sequenceNum' ]

# prefix for the filename of the output file
filenamePrefix = 'expression_result_cell_type'

# global instance of a ExpressionResultCellTypeGatherer
gatherer = ExpressionResultCellTypeGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
