#!/usr/local/bin/python
# 
# gathers data for the 'referenceCounts' table in the front-end database

# NOTE: To add more counts:
#	1. add a fieldname for the count as a global (like MarkerCount)
#	2. add a new query to 'cmds' in the main program
#	3. add processing for the new query to collateResults(), to tie the
#		query results to the new fieldname in each reference's
#		dictionary
#	4. add the new fieldname to fieldOrder in the main program

import Gatherer

###--- Globals ---###

MarkerCount = 'markerCount'

###--- Classes ---###

class ReferenceCountsGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the referenceCounts table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for reference counts,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single reference,
		#	rather than for all references

		if self.keyField == 'referenceKey':
			return 'm._Refs_key = %s' % self.keyValue
		return ''

	def collateResults (self):
		# Purpose: to combine the results of the various queries into
		#	one single list of final results, with one row per
		#	reference

		# list of count types (like field names)
		counts = []

		# initialize dictionary for collecting data per reference
		#	d[reference key] = { count type : count }
		d = {}
		for row in self.results[0]:
			referenceKey = row['_Refs_key']
			d[referenceKey] = {}

		# reference counts
		counts.append (MarkerCount)
		for row in self.results[1]:
			referenceKey = row['_Refs_key']
			d[referenceKey][MarkerCount] = row['']


		# add other counts here...






		# compile the list of collated counts in self.finalResults
		self.finalResults = []
		referenceKeys = d.keys()
		referenceKeys.sort()

		for referenceKey in referenceKeys:
			# get the data we collected for this reference so far
			row = d[referenceKey]

			# add the reference key itself as a field in the row
			row['_Refs_key'] = referenceKey

			# for any count types which had no results for this
			# reference, add a zero count
			for col in counts:
				if not row.has_key(col):
					row[col] = 0
			self.finalResults.append (row)
		return

###--- globals ---###

# remember the %s at the end of each query, so we can do update-by-key when
# needed
cmds = [
	# all references
	'''select m._Refs_key
		from BIB_Refs m %s''',

	# count of references for each reference
	'''select m._Refs_key, count(1)
		from MRK_Reference m %s
		group by m._Refs_key''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [ '_Refs_key', MarkerCount, ]

# prefix for the filename of the output file
filenamePrefix = 'referenceCounts'

# global instance of a ReferenceCountsGatherer
gatherer = ReferenceCountsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
