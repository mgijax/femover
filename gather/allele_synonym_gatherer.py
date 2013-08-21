#!/usr/local/bin/python
# 
# gathers data for the 'allele_synonym' table in the front-end database

import Gatherer

###--- Classes ---###

AlleleSynonymGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the allele_synonym table
	# Has: queries to execute against the source database
	# Does: queries the source database for synonyms for alleles,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	'''select distinct s._Object_key as _Allele_key, s.synonym, t.synonymType
		from mgi_synonym s, mgi_synonymtype t
		where s._SynonymType_key = t._SynonymType_key
			and t._MGIType_key = 11
		order by s._Object_key, s.synonym''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Allele_key', 'synonym', 'synonymType' ]

# prefix for the filename of the output file
filenamePrefix = 'allele_synonym'

# global instance of a AlleleSynonymGatherer
gatherer = AlleleSynonymGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
