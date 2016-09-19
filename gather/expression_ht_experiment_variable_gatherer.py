#!/usr/local/bin/python
# 
# gathers data for the 'expression_ht_experiment_variable' table in the front-end database

import Gatherer
from expression_ht import experiments
from expression_ht import constants as C

###--- Classes ---###

HTExperimentVariableGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the expression_ht_experiment_variable table
	# Has: queries to execute against the source database
	# Does: queries the source database for experiment variables for high-throughput expression
	#	experiments, collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	# TODO - revert back to unhacked form
#	'''select t._Experiment_key, n.term 
#		from %s t, gxd_htexperimentvariable v, voc_term n
#		where t._Experiment_key = v._Experiment_key
#			and v._Term_key = n._Term_key''' % experiments.getExperimentTempTable()
	'''select t._Experiment_key, n.term 
		from %s t, gxd_htexperimentvariable v, voc_term n
		where t._Experiment_key = v._Experiment_key
			and v._Term_key = n._Term_key
		union
		select e._Experiment_key, 'temporary variable for experiment key ' || e._Experiment_key::text
		from %s e
		where not exists (select 1 from gxd_htexperimentvariable v
			where e._Experiment_key = v._Experiment_key)''' % (experiments.getExperimentTempTable(),
				experiments.getExperimentTempTable())
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Experiment_key', 'term',
	]

# prefix for the filename of the output file
filenamePrefix = 'expression_ht_experiment_variable'

# global instance of a HTExperimentVariable Gatherer
gatherer = HTExperimentVariableGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
