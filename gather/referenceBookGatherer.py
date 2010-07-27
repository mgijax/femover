#!/usr/local/bin/python
# 
# gathers data for the 'referenceBook' table in the front-end database

import Gatherer

###--- Classes ---###

ReferenceBookGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the referenceBook table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for books,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	'''select _Refs_key as referenceKey,
		book_au, book_title, place, publisher, series_ed
	from bib_books''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	'referenceKey', 'book_au', 'book_title', 'series_ed', 'place',
	'publisher',
	]

# prefix for the filename of the output file
filenamePrefix = 'referenceBook'

# global instance of a ReferenceBookGatherer
gatherer = ReferenceBookGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
