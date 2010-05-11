#!/usr/local/bin/python
# 
# gathers data for the 'sequenceCounts' table in the front-end database

# NOTE: To add more counts:
#	1. add a fieldname for the count as a global (like MarkerCount)
#	2. add a new query to 'cmds' in the main program
#	3. add processing for the new query to collateResults(), to tie the
#		query results to the new fieldname in each sequence's
#		dictionary
#	4. add the new fieldname to fieldOrder in the main program

import Gatherer

###--- Globals ---###

MarkerCount = 'markerCount'

###--- Classes ---###

class SequenceCountsGatherer (Gatherer.ChunkGatherer):
	# Is: a data gatherer for the sequenceCounts table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for sequence counts,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single sequence,
		#	rather than for all sequences

		if self.keyField == 'sequenceKey':
			return 'm._Sequence_key = %s' % self.keyValue
		return ''

	def collateResults (self):
		# Purpose: to combine the results of the various queries into
		#	one single list of final results, with one row per
		#	sequence

		# list of count types (like field names)
		counts = []

		# initialize dictionary for collecting data per sequence
		#	d[sequence key] = { count type : count }
		d = {}
		for row in self.results[0]:
			sequenceKey = row['_Sequence_key']
			d[sequenceKey] = {}

		# sequence counts
		counts.append (MarkerCount)
		for row in self.results[1]:
			sequenceKey = row['_Sequence_key']
			d[sequenceKey][MarkerCount] = row['']


		# add other counts here...






		# compile the list of collated counts in self.finalResults
		self.finalResults = []
		sequenceKeys = d.keys()
		sequenceKeys.sort()

		for sequenceKey in sequenceKeys:
			# get the data we collected for this sequence so far
			row = d[sequenceKey]

			# add the sequence key itself as a field in the row
			row['_Sequence_key'] = sequenceKey

			# for any count types which had no results for this
			# sequence, add a zero count
			for col in counts:
				if not row.has_key(col):
					row[col] = 0
			self.finalResults.append (row)
		return

	def getMinKeyQuery (self):
		return 'select min(_Sequence_key) from SEQ_Sequence'

	def getMaxKeyQuery (self):
		return 'select max(_Sequence_key) from SEQ_Sequence'

	def getKeyRangeClause (self):
		return 'm._Sequence_key >= %d and m._Sequence_key < %d'

###--- globals ---###

# remember the %s at the end of each query, so we can do update-by-key when
# needed
cmds = [
	# all sequences
	'''select m._Sequence_key
		from SEQ_Sequence m %s''',

	# count of markers for each sequence
	'''select m._Sequence_key, count(1)
		from SEQ_Marker_Cache m %s
		group by m._Sequence_key''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [ '_Sequence_key', MarkerCount, ]

# prefix for the filename of the output file
filenamePrefix = 'sequenceCounts'

# global instance of a SequenceCountsGatherer
gatherer = SequenceCountsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
