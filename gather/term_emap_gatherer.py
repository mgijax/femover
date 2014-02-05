#!/usr/local/bin/python
# 
# gathers data for the 'term_emap' table in the front-end database

import Gatherer

###--- Classes ---###

TermEmapGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the term_emap table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for EMAPA and
	#	EMAPS terms, collates results, writes tab-delimited text file

###--- globals ---###

cmds = [ '''select _Term_key,
		startStage,
		endStage,
		_DefaultParent_key,
		null as stage,
		null as _Emapa_term_key
	from voc_term_emapa
	union
	select _Term_key,
		null as startStage,
		null as endStage,
		_DefaultParent_key,
		stage,
		_Emapa_term_key
	from voc_term_emaps'''	
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	'_Term_key', '_DefaultParent_key', 'startStage', 'endStage',
	'stage', '_Emapa_term_key'
	]

# prefix for the filename of the output file
filenamePrefix = 'term_emap'

# global instance of a TermEmapGatherer
gatherer = TermEmapGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
