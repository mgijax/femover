#!/usr/local/bin/python
# 
# gathers data for the 'probe_note' table in the front-end database

import Gatherer
import logger

###--- Classes ---###

ProbeNoteGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the probe_note table
	# Has: queries to execute against the source database
	# Does: queries the source database for notes for probes,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	# 0. probe and tissue notes; assumes only one note chunk per note
	'''select _Probe_key, 'probe' as note_type, note
		from PRB_Notes
		where note is not null
		union
		select p._Probe_key, 'tissue' as note_type, s.description
		from prb_probe p, prb_source s
		where p._Source_key = s._Source_key
			and s.description is not null
			order by 1, 2''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Probe_key', 'note_type', 'note', ]

# prefix for the filename of the output file
filenamePrefix = 'probe_note'

# global instance of a ProbeNoteGatherer
gatherer = ProbeNoteGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
