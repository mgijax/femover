#!/usr/local/bin/python
# 
# gathers data for the 'term_synonym' table in the front-end database

import Gatherer

###--- Classes ---###

TermSynonymGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the term_synonym table
	# Has: queries to execute against the source database
	# Does: queries the source database for term synonyms,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	'''select s._Object_key as termKey, s.synonym, t.synonymType
	from mgi_synonym s, mgi_synonymtype t
	where s._MGIType_key = 13
		and s._SynonymType_key = t._SynonymType_key''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, 'termKey', 'synonym', 'synonymType'
	]

# prefix for the filename of the output file
filenamePrefix = 'term_synonym'

# global instance of a TermSynonymGatherer
gatherer = TermSynonymGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
