#!/usr/local/bin/python
# 
# gathers data for the 'alleleNote' table in the front-end database

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

class AlleleNoteGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the alleleNote table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for allele notes,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single allele,
		#	rather than for all alleles

		if self.keyField == 'alleleKey':
			return 'mn._Object_key = %s' % self.keyValue
		return ''

	def collateResults (self):
		self.finalResults = []

		m = {}

		for row in self.results[0]:
			alleleKey = row['_Object_key']
			noteType = sybaseUtil.resolve (row['_NoteType_key'],
				'MGI_NoteType', '_NoteType_key', 'noteType')
			note = clean(row['note'])
			noteKey = row['_Note_key']

			r = { 'alleleKey' : alleleKey,
				'noteType' : noteType,
				'note' : note,
				}

			if not m.has_key(alleleKey):
				m[alleleKey] = { noteType : r }
			elif not m[alleleKey].has_key(noteType):
				m[alleleKey][noteType] = r
			else:
				m[alleleKey][noteType]['note'] = \
					m[alleleKey][noteType]['note'] + note 

		alleleKeys = m.keys()
		alleleKeys.sort()

		for alleleKey in alleleKeys:
			notes = m[alleleKey]
			noteTypes = notes.keys()
			noteTypes.sort()

			for noteType in noteTypes:
				row = m[alleleKey][noteType]
				self.finalResults.append (row)
		return

###--- globals ---###

cmds = [
	'''select mn._Object_key, mn._Note_key, mn._NoteType_key, mnc.note
	from MGI_Note mn, MGI_NoteChunk mnc
	where mn._MGIType_key = 11
		and mn._Note_key = mnc._Note_key %s
	order by mn._Object_key, mn._Note_key, mnc.sequenceNum''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'alleleKey', 'noteType', 'note', ]

# prefix for the filename of the output file
filenamePrefix = 'alleleNote'

# global instance of a AlleleNoteGatherer
gatherer = AlleleNoteGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
