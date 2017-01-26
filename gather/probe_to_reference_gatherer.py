#!/usr/local/bin/python
# 
# gathers data for the 'probe_to_reference' table in the front-end database

import Gatherer

###--- Classes ---###

ProbeToReferenceGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the probe_to_reference table
	# Has: queries to execute against the source database
	# Does: queries the source database for probe/reference associations,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	# 0. basic data for probe/reference pair
	'''with extra_counts as (
			select r._Reference_key, count(distinct a._Alias_key) as alias_count,
				count(distinct v._RFLV_key) as polymorphism_count
			from prb_reference r
			left outer join prb_alias a on (r._Reference_key = a._Reference_key)
			left outer join prb_rflv v on (r._Reference_key = v._Reference_key)
			group by 1
			)
		select r._Reference_key, r._Probe_key, r._Refs_key, r.hasRMap, r.hasSequence, n.note, null as qualifier,
			case
				when c.alias_count > 0 then 1
				else 0
				end as hasAliases,
			case
				when c. polymorphism_count > 0 then 1
				else 0
				end as hasPolymorphisms
		from prb_reference r
		inner join extra_counts c on (r._Reference_key = c._Reference_key)
		left outer join prb_ref_notes n on (r._Reference_key = n._Reference_key)''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	'_Reference_key', '_Probe_key', '_Refs_key', 'hasRMap', 'hasSequence', 'hasAliases',
	'hasPolymorphisms', 'note', 'qualifier',
	]

# prefix for the filename of the output file
filenamePrefix = 'probe_to_reference'

# global instance of a ProbeToReferenceGatherer
gatherer = ProbeToReferenceGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
