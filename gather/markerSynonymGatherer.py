#!/usr/local/bin/python
# 
# gathers data for the 'markerSynonym' table in the front-end database

import Gatherer
import sybaseUtil

###--- Classes ---###

class MarkerSynonymGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerSynonym table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for marker synonyms,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single marker,
		#	rather than for all markers

		if self.keyField == 'markerKey':
			return 's._Object_key = %s' % self.keyValue
		return ''

	def postprocessResults (self):
		# Purpose: override to provide key-based lookups

		for r in self.finalResults:
			r['synonymType'] = sybaseUtil.resolve (
				r['_SynonymType_key'])
		return

###--- globals ---###

cmds = [
	'''select s._Object_key as markerKey, s.synonym, s._SynonymType_key,
		r.jnumID
	from MGI_Synonym s, BIB_Citation_Cache r
	where s._MGIType_key = 2
		and s._Refs_key = r._Refs_key %s'''
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, 'markerKey', 'synonym', 'synonymType', 'jnumID'
	]

# prefix for the filename of the output file
filenamePrefix = 'markerSynonym'

# global instance of a MarkerSynonymGatherer
gatherer = MarkerSynonymGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
