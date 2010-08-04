#!/usr/local/bin/python
# 
# gathers data for the 'markerCounts' table in the front-end database

# NOTE: To add more counts:
#	1. add a fieldname for the count as a global (like ReferenceCount)
#	2. add a new query to 'cmds' in the main program
#	3. add processing for the new query to collateResults(), to tie the
#		query results to the new fieldname in each marker's dictionary
#	4. add the new fieldname to fieldOrder in the main program

import Gatherer
import logger

###--- Globals ---###

ReferenceCount = 'referenceCount'
SequenceCount = 'sequenceCount'
AlleleCount = 'alleleCount'

error = 'markerCountsGatherer.error'

###--- Classes ---###

class MarkerCountsGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerCounts table
	# Has: queries to execute against the source database
	# Does: queries the source database for marker counts,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		# Purpose: to combine the results of the various queries into
		#	one single list of final results, with one row per
		#	marker

		# list of count types (like field names)
		counts = []

		# initialize dictionary for collecting data per marker
		#	d[marker key] = { count type : count }
		d = {}
		for row in self.results[0][1]:
			d[row[0]] = {}

		# counts to add in this order, with each tuple being:
		#	(set of results, count constant, count column)

		toAdd = [ (self.results[1], ReferenceCount, 'numRef'),
			(self.results[2], SequenceCount, 'numSeq'),
			(self.results[3], AlleleCount, 'numAll'),
			]

		for (r, countName, colName) in toAdd:
			logger.debug ('Processing %s, %d rows' % (countName,
				len(r[1])) )
			counts.append (countName)
			mrkKeyCol = Gatherer.columnNumber(r[0], '_Marker_key')
			countCol = Gatherer.columnNumber(r[0], colName)

			for row in r[1]:
				mrkKey = row[mrkKeyCol]
				if d.has_key(mrkKey):
					d[mrkKey][countName] = row[countCol]
				else:
					raise error, \
					'Unknown marker key: %d' % mrkKey

		# compile the list of collated counts in self.finalResults
		self.finalResults = []
		markerKeys = d.keys()
		markerKeys.sort()

		self.finalColumns = [ '_Marker_key' ] + counts

		for markerKey in markerKeys:
			row = [ markerKey ]
			for count in counts:
				if d[markerKey].has_key (count):
					row.append (d[markerKey][count])
				else:
					row.append (0)

			self.finalResults.append (row)
		return

###--- globals ---###

# remember the %s at the end of each query, so we can do update-by-key when
# needed
cmds = [
	# all markers
	'''select _Marker_key
		from MRK_Marker''',

	# count of references for each marker
	'''select _Marker_key, count(1) as numRef
		from MRK_Reference
		group by _Marker_key''',

	# count of sequences for each marker
	'''select _Marker_key, count(1) as numSeq
		from SEQ_Marker_Cache
		group by _Marker_key''',

	# count of alleles for each marker
	'''select m._Marker_key, count(1) as numAll
		from ALL_Marker_Assoc m, VOC_Term t
		where m._Status_key = t._Term_key
			and t.term != 'deleted'
		group by _Marker_key''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Marker_key', ReferenceCount, SequenceCount, AlleleCount ]

# prefix for the filename of the output file
filenamePrefix = 'markerCounts'

# global instance of a MarkerCountsGatherer
gatherer = MarkerCountsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
