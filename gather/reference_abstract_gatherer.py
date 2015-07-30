#!/usr/local/bin/python
# 
# gathers data for the 'referenceAbstract' table in the front-end database

import Gatherer

###--- Classes ---###

ReferenceAbstractGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the referenceAbstract table
	# Has: queries to execute against source db
	# Does: queries for primary data for reference abstracts,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	'select _Refs_key as referenceKey, abstract from bib_refs',
	]

# order of fields (from the query results) to be written to the output file
fieldOrder = [ 'referenceKey', 'abstract', ]

# prefix for the filename of the output file
filenamePrefix = 'reference_abstract'

# global instance of a ReferenceAbstractGatherer
gatherer = ReferenceAbstractGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
