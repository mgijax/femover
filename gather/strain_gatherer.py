#!/usr/local/bin/python
# 
# gathers data for the 'strain' table in the front-end database

import Gatherer

###--- Classes ---###

StrainGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the strain table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for strains,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	# 0. Some strains have no preferred ID, others have only a non-MGI preferred ID, and most
	# have at least a preferred MGI ID, while some also have secondary IDs and preferred non-MGI IDs.
	# To choose an ID, we want to:
	#	1. prefer an MGI preferred ID
	#	2. settle for a non-MGI preferred ID, if #1 doesn't exist
	#	3. end up with a null preferred ID, if neither #1 nor #2 exist.
	# Also, we only want to include strains that do not have "involves" in their name.
	'''with minimum_ldb as (
		select _Object_key, min(_LogicalDB_key) as _LogicalDB_key
		from acc_accession
		where _MGIType_key = 10
			and preferred = 1
		group by 1
		)
		select s._Strain_key, s.strain, a.accID
		from prb_strain s
		left outer join acc_accession a on (
			s._Strain_key = a._Object_key
			and a._MGIType_key = 10
			and a.preferred = 1
			and exists (select 1 from minimum_ldb ldb
				where a._Object_key = ldb._Object_key 
					and a._LogicalDB_key = ldb._LogicalDB_key) )
		where s.strain not ilike '%involves%'
		order by 1''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Strain_key', 'strain', 'accID', ]

# prefix for the filename of the output file
filenamePrefix = 'strain'

# global instance of a StrainGatherer
gatherer = StrainGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
