#!/usr/local/bin/python
# 
# gathers data for the 'markerSynonym' table in the front-end database

import Gatherer

###--- Classes ---###

class MarkerSynonymGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerSynonym table
	# Has: queries to execute against the source database
	# Does: queries the source database for marker synonyms,
	#	collates results, writes tab-delimited text file

	def postprocessResults (self):
		# Purpose: override to provide key-based lookups

		self.convertFinalResultsToList()

		typeCol = Gatherer.columnNumber (self.finalColumns,
			'_SynonymType_key')

		for r in self.finalResults:
			self.addColumn ('synonymType',
				Gatherer.resolve (r[typeCol],
					'mgi_synonymtype',
					'_SynonymType_key',
					'synonymType'), r, self.finalColumns)
		return

###--- globals ---###

cmds = [
	'''select s._Object_key as markerKey, s.synonym, s._SynonymType_key,
		r.jnumID
	from mgi_synonym s, bib_citation_cache r
	where s._MGIType_key = 2
		and s._Refs_key = r._Refs_key'''
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, 'markerKey', 'synonym', 'synonymType', 'jnumID'
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_synonym'

# global instance of a MarkerSynonymGatherer
gatherer = MarkerSynonymGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
