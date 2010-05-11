#!/usr/local/bin/python
# 
# gathers data for the 'markerNote' table in the front-end database

import re
import Gatherer
import sybaseUtil

###--- Functions ---###

def clean (s):
	# Purpose: convert tabs and newlines in 's' to be blank spaces

	s = re.sub('\t', ' ', s)
	s = re.sub('\n', ' ', s)
	s = re.sub('\r', ' ', s)
	return s

###--- Classes ---###

class MarkerNoteGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerNote table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for marker notes,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single marker,
		#	rather than for all markers

		if self.keyField == 'markerKey':
			return 'mn._Object_key = %s' % self.keyValue
		return ''

	def collateResults (self):
		self.finalResults = []

		m = {}

		for row in self.results[0]:
			markerKey = row['_Object_key']
			noteType = sybaseUtil.resolve (row['_NoteType_key'],
				'MGI_NoteType', '_NoteType_key', 'noteType')
			note = clean(row['note'])
			noteKey = row['_Note_key']

			r = { 'markerKey' : markerKey,
				'noteType' : noteType,
				'note' : note,
				}

			if not m.has_key(markerKey):
				m[markerKey] = { noteType : r }
			elif not m[markerKey].has_key(noteType):
				m[markerKey][noteType] = r
			else:
				m[markerKey][noteType]['note'] = \
					m[markerKey][noteType]['note'] + note 

		markerKeys = m.keys()
		markerKeys.sort()

		for markerKey in markerKeys:
			notes = m[markerKey]
			noteTypes = notes.keys()
			noteTypes.sort()

			for noteType in noteTypes:
				row = m[markerKey][noteType]
				self.finalResults.append (row)
		return

###--- globals ---###

cmds = [
	'''select mn._Object_key, mn._Note_key, mn._NoteType_key, mnc.note
	from MGI_Note mn, MGI_NoteChunk mnc
	where mn._MGIType_key = 2
		and mn._Note_key = mnc._Note_key %s
	order by mn._Object_key, mn._Note_key, mnc.sequenceNum''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'markerKey', 'noteType', 'note', ]

# prefix for the filename of the output file
filenamePrefix = 'markerNote'

# global instance of a MarkerNoteGatherer
gatherer = MarkerNoteGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
