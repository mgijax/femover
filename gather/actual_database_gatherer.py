#!/usr/local/bin/python
# 
# gathers data for the 'actual_database' table in the front-end database

import Gatherer

###--- Classes ---###

ActualDatabaseGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the actual_database table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for actual dbs,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	'''select adb._ActualDB_key,
		ldb.name as ldbName,
		adb.name as adbName,
		adb.url
	from acc_logicaldb ldb, acc_actualdb adb
	where ldb._LogicalDB_key = adb._LogicalDB_key
		and adb.active = 1
	order by ldb.name, adb.name'''
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_ActualDB_key', 'ldbName', 'adbName', 'url', Gatherer.AUTO
	]

# prefix for the filename of the output file
filenamePrefix = 'actual_database'

# global instance of a ActualDatabaseGatherer
gatherer = ActualDatabaseGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
