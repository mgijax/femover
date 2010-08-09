#!/usr/local/bin/python
# 
# gathers data for the 'Template' table in the front-end database
# (search for all instances of Template to see what to change)

import Gatherer

###--- Classes ---###

ReferenceToSequenceGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the Template table
	# Has: queries to execute against source db
	# Does: queries source db for primary data for Templates,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [''' select _Refs_key, _Object_key, '' as qualifier
	from mgi_reference_assoc
	where _MGIType_key = 19''']

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Object_key', '_Refs_key', 'qualifier'
	]

# prefix for the filename of the output file
filenamePrefix = 'referenceToSequence'

# global instance of a TemplateGatherer
gatherer = ReferenceToSequenceGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
