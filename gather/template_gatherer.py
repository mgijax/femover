#!/usr/local/bin/python
# 
# gathers data for the 'Template' table in the front-end database
# (search for all instances of Template to see what to change)

import Gatherer

###--- Classes ---###

class TemplateGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the Template table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for Templates,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	# Template - fill in necessary SQL commands here.  If this is for a
	# ChunkGatherer, then queries to be processed in chunks require two
	# %d fields, the first for the >= value and the second for < value.
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	# Template - fill in returned fields, in desired order, here.
	# If you need the Gatherer to automatically manage a uniqueKey field
	# in the destination table, specify that field as Gatherer.AUTO.
	]

# prefix for the filename of the output file
filenamePrefix = 'Template'

# global instance of a TemplateGatherer
gatherer = TemplateGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
