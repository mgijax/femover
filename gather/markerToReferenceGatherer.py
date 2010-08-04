#!/usr/local/bin/python
# 
# gathers data for the 'markerToReference' table in the front-end database

import Gatherer

###--- Classes ---###

MarkerToReferenceGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the markerToReference table
	# Has: queries to execute against the source database
	# Does: queries the source database for marker/reference
	#	associations, collates results, writes tab-delimited text file

###--- globals ---###

cmds = [ '''select distinct mr._Marker_key, mr._Refs_key, '' as qualifier
	from MRK_Reference mr, MRK_Marker m 
	where mr._Marker_key = m._Marker_key
		and m._Marker_Status_key != 2''', ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Marker_key', '_Refs_key', 'qualifier', ]

# prefix for the filename of the output file
filenamePrefix = 'markerToReference'

# global instance of a MarkerToReferenceGatherer
gatherer = MarkerToReferenceGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
