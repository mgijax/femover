#!/usr/local/bin/python
# 
# gathers data for the 'antibody_to_reference' table in the front-end database

import Gatherer

###--- Classes ---###

AntibodyToReferenceGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the antibody_to_reference table
	# Has: queries to execute against the source database
	# Does: queries the source database for antibody/reference
	#	associations, collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	'''select r._Object_key as _Antibody_key,
		r._Refs_key,
		t.assocType as qualifier
	from mgi_reference_assoc r, mgi_refassoctype t
	where r._MGIType_key = 6
		and r._RefAssocType_key = t._RefAssocType_key
	''',

	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Antibody_key', '_Refs_key', 'qualifier', ]

# prefix for the filename of the output file
filenamePrefix = 'antibody_to_reference'

# global instance of a AntibodyToReferenceGatherer
gatherer = AntibodyToReferenceGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
