#!/usr/local/bin/python
# 
# gathers data for the 'alleleToReference' table in the front-end database

import Gatherer

###--- Classes ---###

AlleleToReferenceGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the alleleToReference table
	# Has: queries to execute against the source database
	# Does: queries the source database for allele/reference
	#	associations, collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	'''select distinct a._Object_key as _Allele_key, 
		a._Refs_key,
		t.assocType as qualifier
	from mgi_refassoctype t, mgi_reference_assoc a
	where t._RefAssocType_key = a._RefAssocType_key
		and t._MGIType_key = 11
		and exists (select 1 from all_allele aa
			where a._Object_key = aa._Allele_key)''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Allele_key', '_Refs_key', 'qualifier', ]

# prefix for the filename of the output file
filenamePrefix = 'allele_to_reference'

# global instance of a AlleleToReferenceGatherer
gatherer = AlleleToReferenceGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
