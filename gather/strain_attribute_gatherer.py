#!./python
# 
# gathers data for the 'strain_attribute' table in the front-end database

import Gatherer
import StrainUtils

###--- Classes ---###

StrainAttributeGatherer = Gatherer.Gatherer
        # Is: a data gatherer for the strain_attribute table
        # Has: queries to execute against the source database
        # Does: queries the source database for strain attributes (types),
        #       collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
        '''select va._Object_key as _Strain_key, vt.term,
                        row_number() over (order by va._Object_key, vt.term) as row_num
                from voc_annot va, voc_term vt, %s t
                where va._AnnotType_key = 1009
                        and va._Term_key = vt._Term_key
                        and va._Object_key = t._Strain_key
                order by va._Object_key, vt.term''' % StrainUtils.getStrainTempTable(),
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Strain_key', 'term', 'row_num', ]

# prefix for the filename of the output file
filenamePrefix = 'strain_attribute'

# global instance of a StrainAttributeGatherer
gatherer = StrainAttributeGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
