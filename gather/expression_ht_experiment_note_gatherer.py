#!/usr/local/bin/python
# 
# gathers data for the 'expression_ht_experiment_note' table in the front-end database

import Gatherer
from expression_ht import experiments
from expression_ht import constants as C

###--- Classes ---###

HTExperimentNoteGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the expression_ht_experiment_note table
	# Has: queries to execute against the source database
	# Does: queries the source database for notes for high-throughput expression experiments,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	# TODO - revert this back to the non-hacked version
#	'''select t._Experiment_key, y.notetype, c.note
#		from %s t, mgi_note n, mgi_notetype y, mgi_notechunk c
#		where t._Experiment_key = n._Object_key
#			and n._MGIType_key = %d
#			and n._NoteType_key = y._NoteType_key
#			and n._Note_key = c._Note_key
#			and y.private = 0
#		order by t._Experiment_key, y.notetype''' % (
#				experiments.getExperimentTempTable(), C.MGITYPE_EXPERIMENT)
	'''select t._Experiment_key, y.notetype, c.note
		from %s t, mgi_note n, mgi_notetype y, mgi_notechunk c
		where t._Experiment_key = n._Object_key
			and n._MGIType_key = %d
			and n._NoteType_key = y._NoteType_key
			and n._Note_key = c._Note_key
			and y.private = 0
		union
		select t._Experiment_key, 'auto note',
			'Temporary auto-generated note for experiment key ' || t._Experiment_key::text
		from %s t
		where not exists (select 1 from mgi_note n
			where t._Experiment_key = n._Object_key
			and n._MGIType_key = %d)
		order by 1, 2''' % (
				experiments.getExperimentTempTable(), C.MGITYPE_EXPERIMENT,
				experiments.getExperimentTempTable(), C.MGITYPE_EXPERIMENT)
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Experiment_key', 'notetype', 'note' ]

# prefix for the filename of the output file
filenamePrefix = 'expression_ht_experiment_note'

# global instance of a HTExperimentNoteGatherer
gatherer = HTExperimentNoteGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
