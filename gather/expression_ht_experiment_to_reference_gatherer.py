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
        select p._object_key as _Experiment_key, c._Refs_key, null as qualifier
        from MGI_Property p, BIB_Citation_Cache c, VOC_Term t, %s h
        where p._propertyterm_key = t._term_key
        and t.term = 'PubMed ID'
        and p.value = c.pubmedid
        and c.jnumid is not null
        and p._object_key = h._experiment_key
        ''' % experiments.getExperimentTempTable() ,

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
