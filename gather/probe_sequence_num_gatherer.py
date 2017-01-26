#!/usr/local/bin/python
# 
# gathers data for the 'probe_sequence_num' table in the front-end database

import Gatherer
import logger
import symbolsort
import gc

###--- Functions ---###

def nameCompare(a, b):
	# assumes a and b are [name, type, ID, key, ...]
	
	n = symbolsort.nomenCompare(a[0], b[0])
	if n:
		return n
	
	t = symbolsort.nomenCompare(a[1], b[1])
	if t:
		return t
	
	return symbolsort.nomenCompare(a[2], b[2])

def typeCompare(a, b):
	# assumes a and b are [name, type, ID, key, ...]
	
	t = symbolsort.nomenCompare(a[1], b[1])
	if t:
		return t
	
	n = symbolsort.nomenCompare(a[0], b[0])
	if n:
		return n
	
	return symbolsort.nomenCompare(a[2], b[2])

###--- Classes ---###

class ProbeSequenceNumGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the probe_sequence_num table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for probes,
	#	collates results, sorts them, writes tab-delimited text file

	def collateResults(self):
		columns, rows = self.results[0]
	
		toSort = []		# list of [name, type, ID, key] elements

		keyCol = Gatherer.columnNumber(columns, '_Probe_key')
		typeCol = Gatherer.columnNumber(columns, 'type')
		idCol = Gatherer.columnNumber(columns, 'accID')
		nameCol = Gatherer.columnNumber(columns, 'name')

		for row in rows:
			toSort.append ( [ row[nameCol], row[typeCol], row[idCol], row[keyCol] ] )
			
		logger.debug('Collected %d rows to sort' % len(toSort))

		rows = []
		self.results = []
		gc.collect()
		
		logger.debug('Ran garbage collection')
		
		toSort.sort(nameCompare)
		i = 0
		for row in toSort:
			i = i + 1
			row.append(i)
			
		logger.debug('Sorted %d probes by name' % len(toSort))
		
		toSort.sort(typeCompare)
		i = 0
		for row in toSort:
			i = i + 1
			row.append(i)
		
		logger.debug('Sorted %d probes by type' % len(toSort))
		
		self.finalColumns = [ '_Probe_key', 'by_name', 'by_type' ]
		self.finalResults = []

		for row in toSort:
			self.finalResults.append( (row[3], row[4], row[5]) )
			
		toSort = []
		gc.collect()
		
		logger.debug('Collated results; ran garbage collection')
		return
	
###--- globals ---###

cmds = [
	# 0. data needed to sort the probes
	'''select p._Probe_key, t.term as type, a.accID, p.name
		from prb_probe p, voc_term t, acc_accession a
		where a._LogicalDB_key = 1
			and p._Probe_key = a._Object_key
			and a._MGIType_key = 3
			and a.preferred = 1
			and p._SegmentType_key = t._Term_key''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Probe_key', 'by_name', 'by_type' ]

# prefix for the filename of the output file
filenamePrefix = 'probe_sequence_num'

# global instance of a ProbeSequenceNumGatherer
gatherer = ProbeSequenceNumGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
