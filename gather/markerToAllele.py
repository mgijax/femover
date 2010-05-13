#!/usr/local/bin/python
# 
# gathers data for the 'markerToAllele' table in the front-end database

import Gatherer
import sybaseUtil

###--- Classes ---###

class MarkerToAlleleGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerToAllele table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for marker/allele
	#	relationships, collates results, writes tab-delimited text
	#	file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single marker or a
		#	single allele, rather than for all relationships

		if self.keyField == 'markerKey':
			return 'm._Marker_key = %s' % self.keyValue
		elif self.keyField == 'alleleKey':
			return 'm._Allele_key = %s' % self.keyValue
		return ''

	def postprocessResults (self):
		for r in self.finalResults:
			r['qualifier'] = sybaseUtil.resolve (
				r['_Qualifier_key'])
		return

###--- globals ---###

cmds = [
	'''select m._Marker_key,
		m._Allele_key,
		m._Refs_key,
		m._Qualifier_key
	from ALL_Marker_Assoc m, VOC_Term t
	where m._Status_key = t._Term_key
		and t.term != "deleted" %s''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', '_Allele_key', '_Refs_key',
	'qualifier'
	]

# prefix for the filename of the output file
filenamePrefix = 'markerToAllele'

# global instance of a MarkerToAlleleGatherer
gatherer = MarkerToAlleleGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
