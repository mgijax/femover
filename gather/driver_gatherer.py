#!/usr/local/bin/python
# 
# gathers data for the 'driver' table in the front-end database

import Gatherer

###--- Classes ---###

DriverGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the driver table
	# Has: queries to execute against the source database
	# Does: queries the source database for recombinase driver data 

###--- globals ---###

cmds = [
	'''select distinct driverNote as driver
	   from ALL_Cre_Cache 
	   order by driverNote'''
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'driver' ]

# prefix for the filename of the output file
filenamePrefix = 'driver'

# global instance of a DriverGatherer
gatherer = DriverGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
