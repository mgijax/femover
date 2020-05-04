#!./python
# 
# gathers data for the 'mapping_insitu' table in the front-end database

import Gatherer

###--- Classes ---###

MappingInSituGatherer = Gatherer.Gatherer
        # Is: a data gatherer for the mapping_insitu table
        # Has: queries to execute against the source database
        # Does: queries the source database for supplemental data for IN SITU mapping experiments,
        #       collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
        '''select e._Expt_key, s.strain, e.band, e.cellorigin, e.karyotype, e.robertsonians, e.nummetaphase,
                        e.totalgrains, e.grainsonchrom, e.grainsotherchrom
                from MLD_InSitu e, PRB_Strain s
                where e._Strain_key = s._Strain_key''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        Gatherer.AUTO, '_Expt_key', 'strain', 'band', 'cellorigin', 'karyotype', 'robertsonians', 'nummetaphase',
        'totalgrains', 'grainsonchrom', 'grainsotherchrom',
        ]

# prefix for the filename of the output file
filenamePrefix = 'mapping_insitu'

# global instance of a MappingInSituGatherer
gatherer = MappingInSituGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
