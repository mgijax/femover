#!/usr/local/bin/python
# 
# gathers data for the 'markerOrthology' table in the front-end database

import Gatherer
import sybaseUtil

###--- Classes ---###

class MarkerOrthologyGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerOrthology table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for orthologies,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single marker,
		#	rather than for all markers

		if self.keyField == 'markerKey':
			return 'mouse._Marker_key = %s' % self.keyValue
		return ''

	def postprocessResults (self):
		# Purpose: override to provide key-based lookups

		for r in self.finalResults:
			r['otherOrganism'] = sybaseUtil.resolve (
				r['_Organism_key'], 'MGI_Organism',
				'_Organism_key', 'commonName')
		return

###--- globals ---###

cmds = [
	'''select distinct mouse._Marker_key as mouseMarkerKey,
		nonmouse._Marker_key as otherMarkerKey,
		mm.symbol as otherSymbol,
		nonmouse._Organism_key
	from MRK_Homology_Cache mouse,
		MRK_Homology_Cache nonmouse,
		MRK_Marker mm
	where mouse._Organism_key = 1
		and mouse._Class_key = nonmouse._Class_key
		and nonmouse._Organism_key != 1
		and nonmouse._Marker_key = mm._Marker_key %s''',
	]

# order of fields (from the Sybase query results) to be written to the
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
