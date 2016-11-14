#!/usr/local/bin/python
# 
# gathers data for the 'expression_ht_experiment_property' table in the front-end database

import Gatherer
import logger
from expression_ht import experiments
from expression_ht import constants as C

###--- Classes ---###

class HTExperimentPropertyGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the expression_ht_experiment_property table
	# Has: queries to execute against the source database
	# Does: queries the source database for properties associated with high-throughput
	#	expression experiments,	collates results, writes tab-delimited text file
	
	def postprocessResults(self):
		self.convertFinalResultsToList()
		self.finalColumns.append("sequenceNum")
		i = 0
		for row in self.finalResults:
			i = i + 1
			row.append(i)
		logger.debug("Added sequence numbers for %d properties" % len(self.finalResults))
		return

###--- globals ---###

cmds = [
	# note that we only move PubMed IDs to the front-end, no other properties
	'''select p._Property_key, t._Experiment_key, n.term as name, p.value, p.sequenceNum as initialSeqNum
		from %s t, mgi_property p, voc_term n
		where t._Experiment_key = p._Object_key
			and p._MGIType_key = %d
			and p._PropertyTerm_key = n._Term_key
			and n.term = 'PubMed ID'
		order by t._Experiment_key, p.sequenceNum, n.term, p.value''' % (
				experiments.getExperimentTempTable(), C.MGITYPE_EXPERIMENT)
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
		'_Property_key', '_Experiment_key', 'name', 'value', 'sequenceNum'
	]

# prefix for the filename of the output file
filenamePrefix = 'expression_ht_experiment_property'

# global instance of a HTExperimentProperty Gatherer
gatherer = HTExperimentPropertyGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
