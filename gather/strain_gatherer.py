#!/usr/local/bin/python
# 
# gathers data for the 'strain' table in the front-end database

import Gatherer
from strain_marker_gatherer import STRAIN_ORDER
import StrainUtils

###--- Classes ---###

StrainGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the strain table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for strains,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	# 0. load a temp table with sequence keys for the 19 sequenced strains
	'''select _Object_key as _Strain_key
		into temp table sequenced_strains
		from acc_accession
		where _MGIType_key = 10
			and accID in ('%s')''' % "','".join(STRAIN_ORDER),
		
	# 0. Some strains have no preferred ID, others have only a non-MGI preferred ID, and most
	# have at least a preferred MGI ID, while some also have secondary IDs and preferred non-MGI IDs.
	# To choose an ID, we want to:
	#	1. prefer an MGI preferred ID
	#	2. settle for a non-MGI preferred ID, if #1 doesn't exist
	#	3. end up with a null preferred ID, if neither #1 nor #2 exist.
	'''with minimum_ldb as (
		select _Object_key, min(_LogicalDB_key) as _LogicalDB_key
		from acc_accession
		where _MGIType_key = 10
			and preferred = 1
		group by 1
		)
		select s._Strain_key, s.strain, a.accID, st.term as strain_type,
			sp.term as species, s.standard, case
				when ss._Strain_key is null then 0
				else 1
				end as is_sequenced
		from prb_strain s
		inner join voc_term st on (s._StrainType_key = st._Term_key)
		inner join voc_term sp on (s._Species_key = sp._Term_key)
		inner join %s t on (s._Strain_key = t._Strain_key)
		left outer join sequenced_strains ss on (s._Strain_key = ss._Strain_key) 
		left outer join acc_accession a on (
			s._Strain_key = a._Object_key
			and a._MGIType_key = 10
			and a.preferred = 1
			and exists (select 1 from minimum_ldb ldb
				where a._Object_key = ldb._Object_key 
					and a._LogicalDB_key = ldb._LogicalDB_key) )
		order by 1''' % StrainUtils.getStrainTempTable(),
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Strain_key', 'strain', 'accID', 'strain_type', 'species', 'standard', 'is_sequenced']

# prefix for the filename of the output file
filenamePrefix = 'strain'

# global instance of a StrainGatherer
gatherer = StrainGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
