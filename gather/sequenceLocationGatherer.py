#!/usr/local/bin/python
# 
# gathers data for the 'sequenceLocation' table in the front-end database

import Gatherer
import sybaseUtil

###--- Classes ---###

class SequenceLocationGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the sequenceLocation table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for sequence locations,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single sequence,
		#	rather than for all sequences

		if self.keyField == 'sequenceKey':
			return 's._Sequence_key = %s' % self.keyValue
		return ''

	def postprocessResults (self):
		# Purpose: override to provide key-based lookups

		for r in self.finalResults:
			r['buildIdentifier'] = sybaseUtil.resolve (
				r['_Map_key'], 'MAP_Coordinate',
				'_Map_key', 'name')
		return

###--- globals ---###

cmds = [
	'''select s._Sequence_key,
		1 as sequenceNum,
		s.chromosome,
		s.startCoordinate,
		s.endCoordinate,
		s._Map_key,
		'coordinates' as locationType,
		s.mapUnits,
		s.provider,
		s.version
	from SEQ_Coord_Cache s %s''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Sequence_key', 'sequenceNum', 'chromosome',
	'startCoordinate', 'endCoordinate', 'buildIdentifier',
	'locationType', 'mapUnits', 'provider', 'version',
	]

# prefix for the filename of the output file
filenamePrefix = 'sequenceLocation'

# global instance of a SequenceLocationGatherer
gatherer = SequenceLocationGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
