#!/usr/local/bin/python
# 
# gathers data for the 'markerToSequence' table in the front-end database

import Gatherer

###--- Classes ---###

MarkerToSequenceGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the markerToSequence table
	# Has: queries to execute against the source database
	# Does: queries the source database for marker/sequence
	#	relationships, collates results, writes tab-delimited text
	#	file

###--- globals ---###

cmds = [
	'''select m._Marker_key,
		m._Sequence_key,
		m._Refs_key,
		m._Qualifier_key,
		t.term as qualifier
	from seq_marker_cache m, voc_term t
	where m._Qualifier_key = t._Term_key''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', '_Sequence_key', '_Refs_key',
	'qualifier'
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_to_sequence'

# global instance of a MarkerToSequenceGatherer
gatherer = MarkerToSequenceGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
