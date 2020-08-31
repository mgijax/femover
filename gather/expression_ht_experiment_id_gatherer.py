#!./python
# 
# gathers data for the 'expression_ht_experiment_id' table in the front-end database

import Gatherer
import logger
from expression_ht import experiments
from expression_ht import constants as C

###--- Classes ---###

class HTExperimentIDGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the expression_ht_experiment_id table
        # Has: queries to execute against the source database
        # Does: queries the source database for IDs associated with high-throughput
        #       expression experiments, collates results, writes tab-delimited text file

        def postprocessResults(self):
                self.convertFinalResultsToList()
                self.finalColumns.append("sequence_num")
                i = 0
                for row in self.finalResults:
                        i = i + 1
                        row.append(i)
                logger.debug("Added sequence numbers for %d IDs" % len(self.finalResults))
                return
        
###--- globals ---###

cmds = [
        '''select t._Experiment_key, l.name as logical_db, a.accID, a.preferred, a.private
                from %s t, acc_accession a, acc_logicaldb l
                where t._Experiment_key = a._Object_key
                        and a._MGIType_key = %d
                        and a._LogicalDB_key = l._LogicalDB_key
                order by t._Experiment_key, a.preferred desc, a.accID''' % (
                                experiments.getExperimentTempTable(), C.MGITYPE_EXPERIMENT)
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        Gatherer.AUTO, '_Experiment_key', 'logical_db', 'accID', 'preferred', 'private', 'sequence_num',
        ]

# prefix for the filename of the output file
filenamePrefix = 'expression_ht_experiment_id'

# global instance of a HTExperimentIDGatherer
gatherer = HTExperimentIDGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
