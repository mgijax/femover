#!/usr/local/bin/python
# 
# gathers data for the 'markerNote' table in the front-end database

import Gatherer
import logger
import gc

###--- Globals ---###

MP_MARKER = 1015	# VOC_AnnotType : MP/Marker (Derived)
OMIM_MARKER = 1016	# VOC_AnnotType : OMIM/Marker (Derived)
NOT_QUALIFIER = 1614157	# VOC_Term NOT
ANNOT_EVIDENCE = 25	# MGI Type for annotation evidence
VOCAB_TERM = 13		# MGI Type for vocabulary terms

###--- Classes ---###

class MarkerNoteGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerNote table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for marker notes,
	#	collates results, writes tab-delimited text file

	def getRolledUpNotes (self):
		# build a cache of notes for annotations rolled up to markers
		# Returns: { marker key : single consolidated note }

		# assumes ordering by marker key, note number, and then
		# sequence number
		cols, rows = self.results[2]
		
		markerCol = Gatherer.columnNumber (cols, '_Marker_key')
		noteKeyCol = Gatherer.columnNumber (cols, '_Note_key')
		noteCol = Gatherer.columnNumber (cols, 'note')
		seqNumCol = Gatherer.columnNumber (cols, 'sequenceNum')

		notes = {}
		noteCount = 0
		lastNoteKey = None

		for row in rows:
			markerKey = row[markerCol]
			noteKey = row[noteKeyCol]
			chunk = row[noteCol]

			if not notes.has_key(markerKey):
				# This chunk starts a new note for a new
				# marker.

				notes[markerKey] = [ chunk ]
				lastNoteKey = noteKey
				noteCount = noteCount + 1

			elif lastNoteKey == noteKey:
				# This chunk continues the prior note for the
				# same marker.

				notes[markerKey][-1] = notes[markerKey][-1] \
					+ chunk

			else:
				# This chunk starts a new note for an existing
				# marker.

				notes[markerKey].append(chunk)
				lastNoteKey = noteKey
				noteCount = noteCount + 1

		logger.debug ('Collected %d rolled-up notes for %d markers' \
			% (noteCount, len(notes)) )

		# need to now trim whitespace from the notes and skip any
		# duplicate notes for each marker

		minNotes = {}
		noteCount = 0

		for markerKey in notes.keys():
			minNotes[markerKey] = []

			for note in notes[markerKey]:
				minNote = note.strip()

				if minNote not in minNotes[markerKey]:
					minNotes[markerKey].append(minNote)
					noteCount = noteCount + 1

		logger.debug('Removed duplicates; now %d rolled-up notes' \
			% noteCount)

		del notes
		gc.collect()

		# and, pull them into a single string of notes for each marker

		singleNotes = {}

		for markerKey in minNotes.keys():
			singleNotes[markerKey] = ' '.join(minNotes[markerKey])

		logger.debug('Mashed rolled-up notes down to one per marker')
		return singleNotes

	def collateResults (self):
		# m[marker key] = { note type : note }
		m = {}

		# general notes, using MGI_Note tables

		keyCol = Gatherer.columnNumber (self.results[0][0],
			'_Object_key')
		typeCol = Gatherer.columnNumber (self.results[0][0],
			'_NoteType_key')
		noteCol = Gatherer.columnNumber (self.results[0][0], 'note') 

		for row in self.results[0][1]:
			markerKey = row[keyCol]
			noteType = Gatherer.resolve (row[typeCol],
				'mgi_notetype', '_NoteType_key', 'noteType')
			note = row[noteCol]

			if not m.has_key(markerKey):
				m[markerKey] = { noteType : note }
			elif not m[markerKey].has_key(noteType):
				m[markerKey][noteType] = note
			else:
				m[markerKey][noteType] = \
					m[markerKey][noteType] + note 

		logger.debug('Processed %d general notes' % \
			len(self.results[0][1]) )

		# marker clips, using MRK_Notes table

		keyCol = Gatherer.columnNumber (self.results[1][0],
			'_Marker_key')
		noteCol = Gatherer.columnNumber (self.results[1][0], 'note')
		noteType = 'marker clip'

		for row in self.results[1][1]:
			markerKey = row[keyCol]
			note = row[noteCol]

			if not m.has_key(markerKey):
				m[markerKey] = { noteType : note }
			elif not m[markerKey].has_key(noteType):
				m[markerKey][noteType] = note
			else:
				m[markerKey][noteType] = \
					m[markerKey][noteType] + note 

		logger.debug('Processed %d marker clips' % \
			len(self.results[1][1]) )

		# bring in the notes from rolled-up annotations

		rollupNotes = self.getRolledUpNotes()

		noteType = 'rolled up annotation notes'

		for markerKey in rollupNotes.keys():
			note = rollupNotes[markerKey]

			if not m.has_key(markerKey):
				m[markerKey] = { noteType : note }
			else:
				m[markerKey][noteType] = note

		# collate into final results

		self.finalResults = []
		self.finalColumns = [ 'markerKey', 'noteType', 'note' ]

		markerKeys = m.keys()
		markerKeys.sort()

		for markerKey in markerKeys:
			noteTypes = m[markerKey].keys()
			noteTypes.sort()

			# trim trailing whitespace from notes
			for noteType in noteTypes:
				row = [ markerKey, noteType,
					m[markerKey][noteType].rstrip() ]
				self.finalResults.append (row)
		return

###--- globals ---###

cmds = [
	# 0. general notes
	'''select mn._Object_key, mn._Note_key, mn._NoteType_key, mnc.note
	from mgi_note mn, mgi_notechunk mnc
	where mn._MGIType_key = 2
		and mn._Note_key = mnc._Note_key
		and exists (select 1 from mrk_marker m
			where m._Marker_key = mn._Object_key)
	order by mn._Object_key, mn._Note_key, mnc.sequenceNum''',

	# 1. marker clips
	'''select _Marker_key, sequenceNum, note
		from mrk_notes n
		where exists (select 1 from mrk_marker m
			where m._Marker_key = n._Marker_key)
		order by _Marker_key, sequenceNum''',

	# 2. searchable notes for rolled-up OMIM and MP annotations
	'''with rolled_up_annotations as (
		select distinct va._Annot_key,
			va._Term_key,
			ve._AnnotEvidence_key,
			va._Object_key as _Marker_key
		from voc_annot va,
			voc_evidence ve,
			voc_term q
		where va._AnnotType_key = %d
			and va._Qualifier_key = q._Term_key
			and q.term is null
			and va._Annot_key = ve._Annot_key
		union
		select distinct va._Annot_key,
			va._Term_key,
			ve._AnnotEvidence_key,
			va._Object_key as _Marker_key
		from voc_annot va,
			voc_evidence ve
		where va._AnnotType_key = %d
			and va._Annot_key = ve._Annot_key
			and va._Qualifier_key != %d)
	select r._Marker_key,
		n._Note_key,
		c.note,
		c.sequenceNum
	from rolled_up_annotations r,
		mgi_note n,
		mgi_notetype t,
		mgi_notechunk c
	where r._AnnotEvidence_key = n._Object_key
		and n._NoteType_key = t._NoteType_key
		and n._Note_key = c._Note_key
		and t._MGIType_key = %d
	order by r._Marker_key, n._Note_key, c.sequenceNum''' % (MP_MARKER,
		OMIM_MARKER, NOT_QUALIFIER, ANNOT_EVIDENCE)
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'markerKey', 'noteType', 'note', ]

# prefix for the filename of the output file
filenamePrefix = 'marker_note'

# global instance of a MarkerNoteGatherer
gatherer = MarkerNoteGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
