#!/usr/local/bin/python
# 
# gathers data for the 'expression_ht_experiment' table in the front-end db

import Gatherer
import logger
from VocabUtils import getTerm
from expression_ht import experiments
from expression_ht import samples

###--- Classes ---###

class HTExperimentGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the expression_ht_experiment table
	# Has: queries to execute against the source database
	# Does: queries the source database for data for high-throughput
	#	expression experiments, collates results, writes tab-delimited
	#	text file
	
	def collateResults(self):
		cols, rows = experiments.getExperimentIDs(True)
		experimentKeyCol = Gatherer.columnNumber(cols, '_Experiment_key')
		idCol = Gatherer.columnNumber(cols, 'accID')

		primaryIDs = {}
		for row in rows:
			primaryIDs[row[experimentKeyCol]] = row[idCol]

		sampleCounts = samples.getSampleCountsByExperiment()

		cols, rows = self.results[0]
		
		experimentKeyCol = Gatherer.columnNumber(cols, '_Experiment_key')
		sourceKeyCol = Gatherer.columnNumber(cols, '_Source_key')
		nameCol = Gatherer.columnNumber(cols, 'name')
		descriptionCol = Gatherer.columnNumber(cols, 'description')
		releaseDateCol = Gatherer.columnNumber(cols, 'release_date')
		lastUpdateDateCol = Gatherer.columnNumber(cols, 'lastupdate_date')
		studyTypeKeyCol = Gatherer.columnNumber(cols, '_StudyType_key')

		self.finalColumns = fieldOrder
		self.finalResults = []

		for row in rows:
			experimentKey = row[experimentKeyCol]

			primaryID = None
			if experimentKey in primaryIDs:
				primaryID = primaryIDs[experimentKey]
				
# TODO - revert this back to the real version, not the hacked one
#			studyType = getTerm(row[studyTypeKeyCol])
			studyType = experiments.getStudyTypeHack(getTerm(row[studyTypeKeyCol]), row[descriptionCol])
			
			self.finalResults.append ([ experimentKey, primaryID, getTerm(row[sourceKeyCol]),
				row[nameCol], row[descriptionCol], row[releaseDateCol], row[lastUpdateDateCol],
				studyType, sampleCounts[experimentKey] ])

		logger.debug ('Compiled %d data rows' % len(self.finalResults))
		return
			
###--- globals ---###

cmds = [
	# 0. basic experiment data
	'''select e._Experiment_key, e._Source_key, e.name, e.description, e.release_date, e.lastupdate_date,
			e._StudyType_key
		from %s t, gxd_htexperiment e
		where t._Experiment_key = e._Experiment_key''' % experiments.getExperimentTempTable()
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	'_Experiment_key', 'accID', 'source', 'name', 'description', 'release_date',
	'lastupdate_date', 'study_type', 'sample_count'
	]

# prefix for the filename of the output file
filenamePrefix = 'expression_ht_experiment'

# global instance of a HTExperimentGatherer
gatherer = HTExperimentGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
