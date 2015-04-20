#!/usr/local/bin/python
# 
# gathers data for the 'marker_to_antibody' table in the front-end database

import Gatherer

###--- Classes ---###

MarkerToAntibodyGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the marker_to_antibody table
	# Has: queries to execute against the source database
	# Does: queries the source database for associations between markers
	#	and antibodies,	collates results, writes tab-delimited file

###--- globals ---###

cmds = [
	'''select distinct _Marker_key,
		_Antibody_key,
		null as refsKey,
		null as qualifier
	from gxd_antibodymarker''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', '_Antibody_key', 'refsKey', 'qualifier'
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_to_antibody'

# global instance of a MarkerToAntibodyGatherer
gatherer = MarkerToAntibodyGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
