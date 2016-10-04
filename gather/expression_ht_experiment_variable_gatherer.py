#!/usr/local/bin/python
# 
# gathers data for the 'expression_ht_experiment_variable' table in the front-end database

import Gatherer
import logger
from expression_ht import experiments
from expression_ht import constants as C

###--- Classes ---###

# TODO - revert back so this class is just an alias for the base class
# HTExperimentVariableGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the expression_ht_experiment_variable table
	# Has: queries to execute against the source database
	# Does: queries the source database for experiment variables for high-throughput expression
	#	experiments, collates results, writes tab-delimited text file

class HTExperimentVariableGatherer (Gatherer.Gatherer):
	def collateResults(self):
		# TODO - remove this method when the class is restored to just
		# being an alias

		cols, rows = self.results[0]

		variables = []
		for row in rows:
			variables.append(row[0])
		logger.debug('Found %d variables' % len(variables))

		self.finalColumns = [ '_Experiment_key', 'term' ]
		self.finalResults = []

		cols, rows = self.results[1]

		keyCol = Gatherer.columnNumber(cols, '_Experiment_key')
		titleCol = Gatherer.columnNumber(cols, 'title')

		titles = {}
		for row in rows:
			titles[row[keyCol]] = row[titleCol].lower()

		allKeys = titles.keys()

		unassignedKeys = {}
		for key in allKeys:
			unassignedKeys[key] = 1

		logger.debug('Collected %d experiment keys' % len(allKeys))

		for variable in variables:
			ct = 0
			varLower = variable.lower()

			for key in allKeys:
				if titles[key].find(varLower) >= 0:
					self.finalResults.append(
						[ key, variable ] )
					ct = ct + 1
					if key in unassignedKeys:
						del unassignedKeys[key]

			logger.debug('Added %s to %d experiments' % (variable, ct))

		if unassignedKeys:
			for key in unassignedKeys.keys():
				self.finalResults.append( [ key, 'Not Curated' ] )
			logger.debug('Added %d Not Curated variables' % len(unassignedKeys))
		return


###--- globals ---###

cmds = [
	# TODO - revert back to unhacked form
#	'''select t._Experiment_key, n.term 
#		from %s t, gxd_htexperimentvariable v, voc_term n
#		where t._Experiment_key = v._Experiment_key
#			and v._Term_key = n._Term_key''' % experiments.getExperimentTempTable()
	'select term from voc_term where _Vocab_key =122',

	'''select t._Experiment_key, e.name as title
		from %s t, gxd_htexperiment e
		where t._Experiment_key = e._Experiment_key''' % \
			experiments.getExperimentTempTable()
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
