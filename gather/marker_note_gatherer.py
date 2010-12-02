#!/usr/local/bin/python
# 
# gathers data for the 'markerNote' table in the front-end database

import Gatherer

###--- Classes ---###

class MarkerNoteGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerNote table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for marker notes,
	#	collates results, writes tab-delimited text file

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
	'''select mn._Object_key, mn._Note_key, mn._NoteType_key, mnc.note
	from mgi_note mn, mgi_notechunk mnc
	where mn._MGIType_key = 2
		and mn._Note_key = mnc._Note_key
	order by mn._Object_key, mn._Note_key, mnc.sequenceNum''',

	'''select _Marker_key, sequenceNum, note
		from mrk_notes
		order by _Marker_key, sequenceNum''',
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
