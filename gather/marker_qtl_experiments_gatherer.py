#!/usr/local/bin/python
# 
# gathers data for the 'marker_qtl_experiments' table in the front-end database

import Gatherer

###--- Classes ---###

class MarkerQtlExperimentsGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the marker_qtl_experiments table
	# Has: queries to execute against the source database
	# Does: queries the source database for notes for QTL mapping
	#	experiments for QTL markers, collates results, writes
	#	tab-delimited text file

	def collateResults (self):
		cols, rows = self.results[0]

		markerCol = Gatherer.columnNumber (cols, '_Marker_key')
		exptCol = Gatherer.columnNumber (cols, '_Expt_key')
		jnumCol = Gatherer.columnNumber (cols, 'accID')
		noteCol = Gatherer.columnNumber (cols, 'note')

		# (marker key, expt key, jnumID) = note
		notes = {}

		for row in rows:
			markerKey = row[markerCol]
			exptKey = row[exptCol]
			jnumID = row[jnumCol]
			noteChunk = row[noteCol]

			key = (markerKey, exptKey, jnumID)

			if notes.has_key(key):
				notes[key] = notes[key] + noteChunk
			else:
				notes[key] = noteChunk

		keys = notes.keys()
		keys.sort()

		seqNum = 0

		self.finalColumns = [ '_Marker_key', '_Expt_key', 'accID',
			'note', 'sequenceNum' ]
		self.finalResults = []

		for (markerKey, exptKey, jnumID) in keys:
			seqNum = seqNum + 1

			self.finalResults.append ( [ markerKey, exptKey,
				jnumID, notes[(markerKey, exptKey, jnumID)],
				seqNum] )
		return

###--- globals ---###

cmds = [
	'''select mem._Marker_key, me._Expt_key, me._Refs_key, ac.accID,
		men.note
	from MLD_Expt_Marker mem, MLD_Expts me, MLD_Expt_Notes men,
		ACC_Accession ac
	where mem._Expt_key = me._Expt_key 
		and me._Expt_key = men._Expt_key
		and me.exptType in ('TEXT-QTL', 'TEXT-QTL-Candidate Genes')
		and me._Refs_key = ac._Object_key
		and ac._MGIType_key = 1
		and ac.prefixPart = 'J:'
		and ac.preferred = 1
	order by mem._Marker_key, mem.sequenceNum, me._Expt_key,
		men.sequenceNum''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', '_Expt_key', 'accID', 'note',
	'sequenceNum',
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_qtl_experiments'

# global instance of a MarkerQtlExperimentsGatherer
gatherer = MarkerQtlExperimentsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
