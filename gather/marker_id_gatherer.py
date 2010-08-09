#!/usr/local/bin/python
# 
# gathers data for the 'markerID' table in the front-end database

import Gatherer

###--- Classes ---###

MarkerIDGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the markerID table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for marker IDs,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	'''select a._Object_key as markerKey, a._LogicalDB_key,
		a.accID, a.preferred, a.private, ldb.name as logicalDB
	from acc_accession a, acc_logicaldb ldb
	where a._MGIType_key = 2
		and a._LogicalDB_key = ldb._LogicalDB_key'''
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'markerKey', 'logicalDB', 'accID', 'preferred',
	'private' ]

# prefix for the filename of the output file
filenamePrefix = 'markerID'

# global instance of a markerIDGatherer
gatherer = MarkerIDGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
