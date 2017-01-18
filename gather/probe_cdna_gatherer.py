#!/usr/local/bin/python
# 
# gathers data for the 'probe_cdna' table in the front-end database

import Gatherer

###--- Classes ---###

ProbeCdnaGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the probe_cdna table
	# Has: queries to execute against the source database
	# Does: queries the source database for data specifically for cDNA probes,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	'''with myProbes as (select distinct p._Probe_key, p._Source_key
			from prb_probe p, voc_term t, prb_marker pm
			where p._SegmentType_key = t._Term_key
				and t.term = 'cDNA'
				and p._Probe_key = pm._Probe_key
				and pm.relationship in ('E', 'P')
			)
		select p._Probe_key, s.age, t.tissue, vt.term as cell_line
		from myProbes p
		inner join prb_source s on (p._Source_key = s._Source_key and s._Organism_key = 1)
		left outer join prb_tissue t on (s._Tissue_key = t._Tissue_key)
		left outer join voc_term vt on (s._CellLine_key = vt._Term_key)''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Probe_key', 'age', 'tissue', 'cell_line' ]

# prefix for the filename of the output file
filenamePrefix = 'probe_cdna'

# global instance of a ProbeCdnaGatherer
gatherer = ProbeCdnaGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
