#!/usr/local/bin/python
# 
# gathers data for the 'expression_ht_sample_note' table in the front-end database

import Gatherer
import logger
from expression_ht import samples
from expression_ht import constants as C

###--- Classes ---###

HTSampleNoteGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the expression_ht_sample_note table
	# Has: queries to execute against the source database
	# Does: queries the source database for notes for samples for high-throughput expression experiments,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	'''select t._Experiment_key, t._Sample_key, y.noteType, c.note
		from %s t, mgi_note n, mgi_notetype y, mgi_notechunk c
		where t._Sample_key = n._Object_key
			and n._MGIType_key = %d
			and n._NoteType_key = y._NoteType_key
			and n._Note_key = c._Note_key
		order by t._Experiment_key''' % (samples.getSampleTempTable(), C.MGITYPE_SAMPLE)
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Sample_key', 'noteType', 'note', ]

# prefix for the filename of the output file
filenamePrefix = 'expression_ht_sample_note'

# global instance of a HTSampleNoteGatherer
gatherer = HTSampleNoteGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
