#!/usr/local/bin/python
# 
# gathers data for the 'marker_alias' table in the front-end database

import Gatherer

###--- Classes ---###

MarkerAliasGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the marker_alias table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for STS markers,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	'''select a._Marker_key, a._Alias_key, m.symbol, aa.accID
	from mrk_alias a,
		acc_accession aa,
		mrk_marker m
	where a._Alias_key = m._Marker_key
		and m._Marker_key = aa._Object_key
		and aa._MGIType_key = 2
		and aa.preferred = 1
		and aa._LogicalDB_key = 1
	order by a._Marker_key, m.symbol'''
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Marker_key', '_Alias_key', 'symbol', 'accID' ]

# prefix for the filename of the output file
filenamePrefix = 'marker_alias'

# global instance of a MarkerAliasGatherer
gatherer = MarkerAliasGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
