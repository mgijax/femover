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

###--- Globals ---###

ReferenceCount = 'referenceCount'
SequenceCount = 'sequenceCount'

###--- Classes ---###

class MarkerCountsGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerCounts table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for marker counts,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single marker,
		#	rather than for all markers

		if self.keyField == 'markerKey':
			return 'm._Marker_key = %s' % self.keyValue
		return ''

	def collateResults (self):
		# Purpose: to combine the results of the various queries into
		#	one single list of final results, with one row per
		#	marker

		# list of count types (like field names)
		counts = []

		# initialize dictionary for collecting data per marker
		#	d[marker key] = { count type : count }
		d = {}
		for row in self.results[0]:
			markerKey = row['_Marker_key']
			d[markerKey] = {}

		# reference counts
		counts.append (ReferenceCount)
		for row in self.results[1]:
			markerKey = row['_Marker_key']
			d[markerKey][ReferenceCount] = row['']

		# sequence counts
		counts.append (SequenceCount)
		for row in self.results[2]:
			markerKey = row['_Marker_key']
			d[markerKey][SequenceCount] = row['']

		# add other counts here...






		# compile the list of collated counts in self.finalResults
		self.finalResults = []
		markerKeys = d.keys()
		markerKeys.sort()

		for markerKey in markerKeys:
			# get the data we collected for this marker so far
			row = d[markerKey]

			# add the marker key itself as a field in the row
			row['_Marker_key'] = markerKey

			# for any count types which had no results for this
			# marker, add a zero count
			for col in counts:
				if not row.has_key(col):
					row[col] = 0
			self.finalResults.append (row)
		return

###--- globals ---###

# remember the %s at the end of each query, so we can do update-by-key when
# needed
cmds = [
	# all markers
	'''select m._Marker_key
		from MRK_Marker m %s''',

	# count of references for each marker
	'''select m._Marker_key, count(1)
		from MRK_Reference m %s
		group by m._Marker_key''',

	# count of sequences for each marker
	'''select m._Marker_key, count(1)
		from SEQ_Marker_Cache m %s
		group by m._Marker_key''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [ '_Marker_key', ReferenceCount, SequenceCount, ]

# prefix for the filename of the output file
filenamePrefix = 'markerCounts'

# global instance of a MarkerCountsGatherer
gatherer = MarkerCountsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
