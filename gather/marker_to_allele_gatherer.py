#!/usr/local/bin/python
# 
# gathers data for the 'markerToAllele' table in the front-end database

import Gatherer

###--- Classes ---###

class MarkerToAlleleGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerToAllele table
	# Has: queries to execute against the source database
	# Does: queries the source database for marker/allele
	#	relationships, collates results, writes tab-delimited text
	#	file
	pass

###--- globals ---###

cmds = [
	'''select a._Marker_key,
		a._Allele_key
	from all_allele a
	where a._Marker_key is not null''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', '_Allele_key'
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_to_allele'

# global instance of a MarkerToAlleleGatherer
gatherer = MarkerToAlleleGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
