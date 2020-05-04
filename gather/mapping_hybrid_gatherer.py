#!./python
# 
# gathers data for the 'mapping_hybrid' table in the front-end database

import Gatherer

###--- Classes ---###

MappingHybridGatherer = Gatherer.Gatherer
        # Is: a data gatherer for the mapping_hybrid table
        # Has: queries to execute against the source database
        # Does: queries the source database for supplemental data for HYBRID mapping experiments,
        #       collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
        '''select _Expt_key, band, case
                        when chrsOrGenes = 0 then 'chromosomes'
                        else'markers' 
                        end as concordance_type
                from MLD_Hybrid''' 
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Expt_key', 'band', 'concordance_type', ]

# prefix for the filename of the output file
filenamePrefix = 'mapping_hybrid'

# global instance of a MappingHybridGatherer
gatherer = MappingHybridGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
