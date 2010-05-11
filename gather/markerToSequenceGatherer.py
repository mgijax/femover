#!/usr/local/bin/python
# 
# gathers data for the 'markerToSequence' table in the front-end database

import Gatherer
import sybaseUtil

###--- Classes ---###

class MarkerToSequenceGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerToSequence table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for marker/sequence
	#	relationships, collates results, writes tab-delimited text
	#	file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single marker or a
		#	single sequence, rather than for all relationships

		if self.keyField == 'markerKey':
			return 'm._Marker_key = %s' % self.keyValue
		elif self.keyField == 'sequenceKey':
			return 'm._Sequence_key = %s' % self.keyValue
		return ''

	def postprocessResults (self):
		for r in self.finalResults:
			r['qualifier'] = sybaseUtil.resolve (
				r['_Qualifier_key'])
		return

###--- globals ---###

cmds = [
	'''select m._Marker_key,
		m._Sequence_key,
		m._Refs_key,
		m._Qualifier_key
	from SEQ_Marker_Cache m %s''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', '_Sequence_key', '_Refs_key',
	'qualifier'
	]

# prefix for the filename of the output file
filenamePrefix = 'markerToSequence'

# global instance of a MarkerToSequenceGatherer
gatherer = MarkerToSequenceGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
