#!/usr/local/bin/python
# 
# gathers data for the 'term_note' table in the front-end database

import Gatherer

###--- Classes ---###

TermNoteGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the term_note table
	# Has: queries to execute against the source database
	# Does: queries the source database for public notes for vocabulary terms,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	# 0. start with comments for GO terms
	'''select tt._Term_key, c.note, case 
			when t.noteType = 'Public Vocabulary Term Comment' then 'Comment'
			else t.noteType
			end as noteType
		from mgi_notetype t, mgi_note n, voc_term tt, voc_vocab v, mgi_notechunk c
		where t._MGIType_key = 13
			and t._NoteType_key = n._NoteType_key
			and n._Object_key = tt._Term_key
			and n._Note_key = c._Note_key
			and tt._Vocab_key = v._Vocab_key
			and v.name = 'GO' ''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Term_key', 'noteType', 'note', ]

# prefix for the filename of the output file
filenamePrefix = 'term_note'

# global instance of a TermNoteGatherer
gatherer = TermNoteGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
