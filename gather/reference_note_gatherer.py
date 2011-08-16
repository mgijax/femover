#!/usr/local/bin/python
# 
# gathers data for the 'reference_note' table in the front-end database

import Gatherer
import logger
import config
import dbAgnostic

###--- Functions ---###

def jnumToKey (jnum):
	cols, rows = dbAgnostic.execute ('''select _Object_key
		from acc_accession
		where _MGIType_key = 1
			and accID = '%s'
			and prefixPart = 'J:' ''' % jnum)
	if rows:
		return rows[0][0]
	return None

def getFullTextLinks():
	keyToURL = {}

	if config.__dict__.has_key('FULL_TEXT_LINKS'):
		try:
			fp = open (config.FULL_TEXT_LINKS, 'r')
			lines = fp.readlines()
			fp.close()
		except:
			raise Gatherer.Error, 'Could not read from %s' % \
				config.FULL_TEXT_LINKS

		mappings = []
		for line in lines:
			line = line.strip()

			# skip comments and blank lines
			if (line[0] == '#') or (not line):
				continue

			items = line.split(' ')
			if len(items) < 2:
				logger.debug ('Flawed line in %s: %s' % (
					config.FULL_TEXT_LINKS, line))
				continue

			jnum = items[0]
			refsKey = jnumToKey(jnum)
			url = ' '.join(items[1:])

			if not refsKey:
				continue

			keyToURL[refsKey] = url
	else:
		logger.debug ('FULL_TEXT_LINKS not defined; skipping')

	return keyToURL

###--- Classes ---###

class MarkerNoteGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the reference_note table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for reference
	#	notes, collates results, writes tab-delimited text file

	def collateResults (self):
		# m[reference key] = { note type : note }
		m = {}

		# general notes, using MGI_Note tables

		keyCol = Gatherer.columnNumber (self.results[0][0],
			'_Object_key')
		typeCol = Gatherer.columnNumber (self.results[0][0],
			'_NoteType_key')
		noteCol = Gatherer.columnNumber (self.results[0][0], 'note') 

		for row in self.results[0][1]:
			referenceKey = row[keyCol]
			noteType = Gatherer.resolve (row[typeCol],
				'mgi_notetype', '_NoteType_key', 'noteType')
			note = row[noteCol]

			if not m.has_key(referenceKey):
				m[referenceKey] = { noteType : note }
			elif not m[referenceKey].has_key(noteType):
				m[referenceKey][noteType] = note
			else:
				m[referenceKey][noteType] = \
					m[referenceKey][noteType] + note 

		# full text links from external data file

		fullTextLinks = getFullTextLinks()
		fullTextType = 'Full Text'

		for (key, url) in fullTextLinks.items():
			if not m.has_key(key):
				m[key] = { fullTextType : url }
			else:
				m[key][fullTextType] = url

		# collate into final results

		self.finalResults = []
		self.finalColumns = [ 'referenceKey', 'noteType', 'note' ]

		referenceKeys = m.keys()
		referenceKeys.sort()

		for referenceKey in referenceKeys:
			noteTypes = m[referenceKey].keys()
			noteTypes.sort()

			# trim trailing whitespace from notes
			for noteType in noteTypes:
				row = [ referenceKey, noteType,
					m[referenceKey][noteType].rstrip() ]
				self.finalResults.append (row)
		return

###--- globals ---###

cmds = [
	'''select mn._Object_key, mn._Note_key, mn._NoteType_key, mnc.note
	from mgi_note mn, mgi_notechunk mnc
	where mn._MGIType_key = 1
		and mn._Note_key = mnc._Note_key
		and exists (select 1 from bib_refs m
			where m._Refs_key = mn._Object_key)
	order by mn._Object_key, mn._Note_key, mnc.sequenceNum''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'referenceKey', 'noteType', 'note', ]

# prefix for the filename of the output file
filenamePrefix = 'reference_note'

# global instance of a MarkerNoteGatherer
gatherer = MarkerNoteGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
