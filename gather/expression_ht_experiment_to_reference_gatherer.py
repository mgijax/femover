#!./python
# 
# gathers data for the 'expression_ht_experiment_to_reference' table in the front-end database

import Gatherer
from expression_ht import experiments

###--- Classes ---###

ExpressionHtExperimentToReferenceGatherer = Gatherer.Gatherer
        # Is: a data gatherer for the antibody_to_reference table
        # Has: queries to execute against the source database
        # Does: queries the source database for antibody/reference
        #       associations, collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
        '''
        select _Experiment_key, _Refs_key, null as qualifier
        from %s
        ''' % experiments.getExperimentReferenceTempTable() ,
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Experiment_key', '_Refs_key', 'qualifier', ]

# prefix for the filename of the output file
filenamePrefix = 'expression_ht_experiment_to_reference'

# global instance of a AntibodyToReferenceGatherer
gatherer = ExpressionHtExperimentToReferenceGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
