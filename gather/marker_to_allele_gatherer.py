#!/usr/local/bin/python
# 
# gathers data for the 'markerToAllele' table in the front-end database

import Gatherer

###--- Classes ---###

class MarkerToAlleleGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerToAllele table
	# Has: queries to execute against the source database
	# Does: queries the source database for marker/allele
	#	relationships, collates results, writes tab-delimited text
	#	file

	def postprocessResults (self):
		self.convertFinalResultsToList()

		qCol = Gatherer.columnNumber (self.finalColumns,
			'_Qualifier_key')

		for r in self.finalResults:
			self.addColumn ('qualifier', Gatherer.resolve (
				r[qCol]), r, self.finalColumns)
		return

###--- globals ---###

cmds = [
	# we only want alleles that are actually in the allele table...
	'''select m._Marker_key,
		m._Allele_key,
		m._Refs_key,
		m._Qualifier_key
	from all_marker_assoc m, voc_term t
	where m._Status_key = t._Term_key
		and t.term != 'deleted'
		and exists (select 1 from all_allele a
			where a._Allele_key = m._Allele_key)''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', '_Allele_key', '_Refs_key',
	'qualifier'
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_to_allele'

# global instance of a MarkerToAlleleGatherer
gatherer = MarkerToAlleleGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
