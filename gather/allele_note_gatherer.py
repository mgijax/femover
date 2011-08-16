#!/usr/local/bin/python
# 
# gathers data for the 'alleleNote' table in the front-end database

import Gatherer

###--- Classes ---###

class AlleleNoteGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the alleleNote table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for allele notes,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		# m[alleleKey] = { noteType : note }
		m = {}

		keyCol = Gatherer.columnNumber (self.results[0][0],
			'_Object_key')
		typeCol = Gatherer.columnNumber (self.results[0][0],
			'_NoteType_key')
		noteCol = Gatherer.columnNumber (self.results[0][0], 'note')

		for row in self.results[0][1]:
			alleleKey = row[keyCol]
			noteType = Gatherer.resolve (row[typeCol],
				'mgi_notetype', '_NoteType_key', 'noteType')
			note = row[noteCol]

			if not m.has_key(alleleKey):
				m[alleleKey] = { noteType : note }
			elif not m[alleleKey].has_key(noteType):
				m[alleleKey][noteType] = note
			else:
				m[alleleKey][noteType] = \
					m[alleleKey][noteType] + note 

		self.finalColumns = [ 'alleleKey', 'noteType', 'note' ]
		self.finalResults = []

		alleleKeys = m.keys()
		alleleKeys.sort()

		for alleleKey in alleleKeys:
			noteTypes = m[alleleKey].keys()
			noteTypes.sort()

			# make sure to trim off trailing whitespace

			for noteType in noteTypes:
				row = [ alleleKey, noteType,
					m[alleleKey][noteType].rstrip() ]
				self.finalResults.append (row)
		return

###--- globals ---###

cmds = [
	'''select mn._Object_key, mn._Note_key, mn._NoteType_key, mnc.note
	from mgi_note mn, mgi_notechunk mnc
	where mn._MGIType_key = 11
		and mn._Note_key = mnc._Note_key
		and exists (select 1 from all_allele a
			where a._Allele_key = mn._Object_key)
	order by mn._Object_key, mn._Note_key, mnc.sequenceNum''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'alleleKey', 'noteType', 'note', ]

# prefix for the filename of the output file
filenamePrefix = 'allele_note'

# global instance of a AlleleNoteGatherer
gatherer = AlleleNoteGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
