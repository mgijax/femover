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
ReferenceCount = 'referenceCount'

###--- Classes ---###

class AlleleCountsGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the alleleCounts table
	# Has: queries to execute against the source database
	# Does: queries the source database for allele counts,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		# Purpose: to combine the results of the various queries into
		#	one single list of final results, with one row per
		#	allele

		# list of count types (like field names)
		counts = []

		# initialize dictionary for collecting data per allele
		#	d[allele key] = { count type : count }
		d = {}
		keyCol = Gatherer.columnNumber (self.results[0][0],
			'_Allele_key')
		for row in self.results[0][1]:
			d[row[keyCol]] = {}

		# marker counts
		counts.append (MarkerCount)

		keyCol = Gatherer.columnNumber (self.results[1][0],
			'_Allele_key')
		ctCol = Gatherer.columnNumber (self.results[1][0], 'mrkCount')

		for row in self.results[1][1]:
			d[row[keyCol]][MarkerCount] = row[ctCol]

		# reference counts
		counts.append (ReferenceCount)

		keyCol = Gatherer.columnNumber (self.results[2][0],
			'_Object_key')
		ctCol = Gatherer.columnNumber (self.results[2][0], 'refCount')

		for row in self.results[2][1]:
			d[row[keyCol]][ReferenceCount] = row[ctCol]


		# add other counts here...

		# (see referenceCountsGatherer for a nice pattern to use)




		# compile the list of collated counts in self.finalResults
		self.finalResults = []
		alleleKeys = d.keys()
		alleleKeys.sort()

		self.finalColumns = [ '_Allele_key' ] + counts

		for alleleKey in alleleKeys:
			row = [ alleleKey ]
			for count in counts:
				if d[alleleKey].has_key(count):
					row.append (d[alleleKey][count])
				else:
					row.append (0)

			self.finalResults.append (row)
		return

###--- globals ---###

# remember the %s at the end of each query, so we can do update-by-key when
# needed
cmds = [
	# all alleles
	'''select _Allele_key from all_allele''',

	# count of markers for each allele
	'''select m._Allele_key, count(1) as mrkCount
		from all_marker_assoc m
		group by m._Allele_key''',

	# count of references for each allele
	'''select _Object_key, count(distinct _Refs_key) as refCount
		from mgi_reference_assoc
		where _MGIType_key = 11
		group by _Object_key''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Allele_key', MarkerCount, ReferenceCount, ]

# prefix for the filename of the output file
filenamePrefix = 'allele_counts'

# global instance of a AlleleCountsGatherer
gatherer = AlleleCountsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
