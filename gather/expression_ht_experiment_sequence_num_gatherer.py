#!/usr/local/bin/python
# 
# gathers data for the 'expression_ht_experiment_sequence_num' table in the front-end database

import Gatherer
import logger
import symbolsort
from expression_ht import experiments
from expression_ht import constants as C
from types import ListType

###--- Functions ---###

def smartAlphaSort(a,b):
	# smart alpha sort of items a and b, with the added caveat that None values sort after non-None
	
	if a and b:
		return symbolsort.nomenCompare(a,b)
	elif a:
		return -1
	elif b:
		return 1
	return 0
	
def studyTypeComparison(a,b):
	# assumes a and b are both (study type, name, experiment key)
	# alpha comparison on study type, smart-alpha on name, finally by key
	
	c = cmp(a[0], b[0])
	if c:
		return c
	
	c = smartAlphaSort(a[1], b[1])
	if c:
		return c
	
	return cmp(a[2], b[2])

def listToSeqNumDict(sortedList):
	# converts a sorted list of tuples to a dictionary that maps from experiment key (the last
	# item in each tuple) to the tuple's position in the list.  This is the sequence number that
	# can be used for sorting by the criteria used in sorting the list.
	
	d = {}
	i = 0
	for row in sortedList:
		i = i + 1
		d[row[-1]] = i
		
	return d
		
###--- Classes ---###

class HTExperimentSequenceNumGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the expression_ht_experiment_sequence_num table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for high-throughput expression
	#	experiments, collates results, computes sequences numbers, and writes tab-delimited
	#	text file
	
	def collateResults(self):
		# lists of tuples to compile for sorting
		names = []
		descriptions = []
		studyTypes = []
		ids = []
		
		cols, rows = self.results[0]
		
		keyCol = Gatherer.columnNumber(cols, '_Experiment_key')
		nameCol = Gatherer.columnNumber(cols, 'name')
		descriptionCol = Gatherer.columnNumber(cols, 'description')
		studyTypeCol = Gatherer.columnNumber(cols, 'study_type')

		for row in rows:
			experimentKey = row[keyCol]
			names.append( (row[nameCol], experimentKey) )
			descriptions.append( (row[descriptionCol], experimentKey) )
			studyTypes.append( (row[studyTypeCol], row[nameCol], experimentKey) )
			
		logger.debug('Collected %d names, descriptions, study types' % len(rows))

		cols, rows = experiments.getExperimentIDs(True)
		keyCol = Gatherer.columnNumber(cols, '_Experiment_key')
		idCol = Gatherer.columnNumber(cols, 'accID')
		
		for row in rows:
			ids.append( (row[idCol], row[keyCol]) )
			
		logger.debug('Collected %d primary IDs' % len(rows))
		
		names.sort(lambda a,b : smartAlphaSort(a[0], b[0]))
		descriptions.sort(lambda a,b : smartAlphaSort(a[0], b[0]))
		ids.sort(lambda a,b : smartAlphaSort(a[0], b[0]))
		studyTypes.sort(studyTypeComparison)
			
		logger.debug('Sorted lists of items')
		
		nameDict = listToSeqNumDict(names)
		descriptionDict = listToSeqNumDict(descriptions)
		studyTypeDict = listToSeqNumDict(studyTypes)
		idDict = listToSeqNumDict(ids)
		
		logger.debug('Built dictionaries by key')
		
		self.finalColumns = [ '_Experiment_key', 'byPrimaryID', 'byName', 'byDescription', 'byStudyType' ]
		self.finalResults = []
		
		keys = nameDict.keys()
		keys.sort()
		for key in keys:
			# If an experiment does not have a primary ID, then its ID sort should be to the end; order any
			# of those cases at the end secondarily by name.
			
			if key in idDict:
				idSeqNum = idDict[key]
			else:
				idSeqNum = len(idDict) + nameDict[key]
				
			self.finalResults.append( [ key, idSeqNum, nameDict[key], descriptionDict[key], studyTypeDict[key] ] )
			
		logger.debug('Collated data for %d experiments' % len(self.finalResults))
		return

###--- globals ---###

cmds = [
	'''select t._Experiment_key, e.name, e.description, s.term as study_type
		from %s t
		inner join gxd_htexperiment e on (t._Experiment_key = e._Experiment_key)
		left outer join voc_term s on (e._StudyType_key = s._Term_key)''' % experiments.getExperimentTempTable()
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	'_Experiment_key', 'byPrimaryID', 'byName', 'byDescription', 'byStudyType',
	]

# prefix for the filename of the output file
filenamePrefix = 'expression_ht_experiment_sequence_num'

# global instance of a HTExperimentSequenceNumGatherer
gatherer = HTExperimentSequenceNumGatherer(filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
