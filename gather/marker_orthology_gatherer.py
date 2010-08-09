#!/usr/local/bin/python
# 
# gathers data for the 'markerOrthology' table in the front-end database

import Gatherer

###--- Classes ---###

class MarkerOrthologyGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerOrthology table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for orthologies,
	#	collates results, writes tab-delimited text file

	def postprocessResults (self):
		# Purpose: override to provide key-based lookups

		self.convertFinalResultsToList()

		orgCol = Gatherer.columnNumber (self.finalColumns,
			'_Organism_key')
		for r in self.finalResults:
			self.addColumn ('otherOrganism', Gatherer.resolve (
				r[orgCol], 'mgi_organism',
				'_Organism_key', 'commonName'), r,
				self.finalColumns)
		return

###--- globals ---###

cmds = [
	'''select distinct mouse._Marker_key as mouseMarkerKey,
		nonmouse._Marker_key as otherMarkerKey,
		mm.symbol as otherSymbol,
		nonmouse._Organism_key
	from mrk_homology_cache mouse,
		mrk_homology_cache nonmouse,
		mrk_marker mm
	where mouse._Organism_key = 1
		and mouse._Class_key = nonmouse._Class_key
		and nonmouse._Organism_key != 1
		and nonmouse._Marker_key = mm._Marker_key''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO,
	'mouseMarkerKey', 'otherMarkerKey', 'otherSymbol', 'otherOrganism',
	]

# prefix for the filename of the output file
filenamePrefix = 'markerOrthology'

# global instance of a MarkerOrthologyGatherer
gatherer = MarkerOrthologyGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
