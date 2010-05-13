#!/usr/local/bin/python
# 
# gathers data for the 'alleleCounts' table in the front-end database

# NOTE: To add more counts:
#	1. add a fieldname for the count as a global (like ReferenceCount)
#	2. add a new query to 'cmds' in the main program
#	3. add processing for the new query to collateResults(), to tie the
#		query results to the new fieldname in each allele's dictionary
#	4. add the new fieldname to fieldOrder in the main program

import Gatherer

###--- Globals ---###

MarkerCount = 'markerCount'

###--- Classes ---###

class AlleleCountsGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the alleleCounts table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for allele counts,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single allele,
		#	rather than for all alleles

		if self.keyField == 'alleleKey':
			return 'm._Allele_key = %s' % self.keyValue
		return ''

	def collateResults (self):
		# Purpose: to combine the results of the various queries into
		#	one single list of final results, with one row per
		#	allele

		# list of count types (like field names)
		counts = []

		# initialize dictionary for collecting data per allele
		#	d[allele key] = { count type : count }
		d = {}
		for row in self.results[0]:
			alleleKey = row['_Allele_key']
			d[alleleKey] = {}

		# marker counts
		counts.append (MarkerCount)
		for row in self.results[1]:
			alleleKey = row['_Allele_key']
			d[alleleKey][MarkerCount] = row['']

		# add other counts here...






		# compile the list of collated counts in self.finalResults
		self.finalResults = []
		alleleKeys = d.keys()
		alleleKeys.sort()

		for alleleKey in alleleKeys:
			# get the data we collected for this allele so far
			row = d[alleleKey]

			# add the allele key itself as a field in the row
			row['_Allele_key'] = alleleKey

			# for any count types which had no results for this
			# allele, add a zero count
			for col in counts:
				if not row.has_key(col):
					row[col] = 0
			self.finalResults.append (row)
		return

###--- globals ---###

# remember the %s at the end of each query, so we can do update-by-key when
# needed
cmds = [
	# all alleles
	'''select m._Allele_key
		from ALL_Allele m %s''',

	# count of markers for each allele
	'''select m._Allele_key, count(1)
		from ALL_Marker_Assoc m %s
		group by m._Allele_key''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [ '_Allele_key', MarkerCount, ]

# prefix for the filename of the output file
filenamePrefix = 'alleleCounts'

# global instance of a AlleleCountsGatherer
gatherer = AlleleCountsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
