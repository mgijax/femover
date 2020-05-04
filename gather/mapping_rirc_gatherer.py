#!./python
# 
# gathers data for the 'mapping_rirc' table in the front-end database

import Gatherer

###--- Classes ---###

MappingRIRCGatherer = Gatherer.Gatherer
        # Is: a data gatherer for the mapping_rirc table
        # Has: queries to execute against the source database
        # Does: queries the source database for basic RI/RC set data for mapping experiments,
        #       collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
        '''select e._Expt_key, r.designation, r.abbrev1, r.abbrev2, s1.strain as strain1, s2.strain as strain2
                from MLD_RI e, RI_RISet r, PRB_Strain s1, PRB_Strain s2
                where e._RISet_key = r._RISet_key
                        and r._Strain_key_1 = s1._Strain_key
                        and r._Strain_key_2 = s2._Strain_key''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Expt_key', 'designation', 'abbrev1', 'abbrev2', 'strain1', 'strain2', ]

# prefix for the filename of the output file
filenamePrefix = 'mapping_rirc'

# global instance of a MappingRIRCGatherer
gatherer = MappingRIRCGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
