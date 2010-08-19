#!/usr/local/bin/python
# 
# gathers data for the 'markerToProbe' table in the front-end database

import Gatherer

###--- Classes ---###

MarkerToProbeGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the markerToProbe table
	# Has: queries to execute against the source database
	# Does: queries the source database for probe/sequence
	#	relationships, collates results, writes tab-delimited text
	#	file

###--- globals ---###

cmds = [
	'''select _Probe_key,
		_Marker_key,
		_Refs_key,
		relationship as qualifier
	from prb_marker''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', '_Probe_key', '_Refs_key',
	'qualifier'
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_to_probe'

# global instance of a MarkerToProbeGatherer
gatherer = MarkerToProbeGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
