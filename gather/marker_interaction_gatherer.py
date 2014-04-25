#!/usr/local/bin/python
# 
# gathers data for the 'marker_interaction' and 'marker_interaction_property'
# tables in the front-end database

import sys
import KeyGenerator
import Gatherer
import logger
import types
import dbAgnostic
import ListSorter
import random


###--- Globals ---###

miGenerator = KeyGenerator.KeyGenerator('marker_interaction')

###--- Functions ---###

# marker key -> (genetic chrom, genomic chrom, start coord, end coord)
coordCache = {}	

def populateCache():
	# populate the global 'coordCache' with location data for markers

	global coordCache

	cmd = '''select _Marker_key, genomicChromosome, chromosome,
			startCoordinate, endCoordinate
		from mrk_location_cache'''

	(cols, rows) = dbAgnostic.execute(cmd)

	keyCol = dbAgnostic.columnNumber(cols, '_Marker_key')
	genomicChrCol = dbAgnostic.columnNumber(cols, 'genomicChromosome')
	geneticChrCol = dbAgnostic.columnNumber(cols, 'chromosome')
	startCol = dbAgnostic.columnNumber(cols, 'startCoordinate')
	endCol = dbAgnostic.columnNumber(cols, 'endCoordinate')

	for row in rows:
		coordCache[row[keyCol]] = (row[geneticChrCol],
			row[genomicChrCol], row[startCol], row[endCol])

	logger.debug ('Cached %d locations' % len(coordCache))
	return

def getMarkerCoords(markerKey):
	# get (genetic chrom, genomic chrom, start coord, end coord) for the
	# given marker key

	if len(coordCache) == 0:
		populateCache()

	if coordCache.has_key(markerKey):
		return coordCache[markerKey]

	return (None, None, None, None)

def getChromosome (marker):
	# get the chromosome for the given marker key or ID, preferring
	# the genomic one over the genetic one

	(geneticChr, genomicChr, startCoord, endCoord) = getMarkerCoords(marker)

	if genomicChr:
		return genomicChr
	return geneticChr

def getStartCoord (marker):
	return getMarkerCoords(marker)[2]

def getEndCoord (marker):
	return getMarkerCoords(marker)[3] 

def addInTeaserFlag (cols, rows):
	# add an in_teaser column to each row in rows, where in_teaser = 1 for
	# the first three rows (with distinct regulated markers) for a given
	# marker and in_teaser = 0 otherwise.  Assumes we are sorted primarily
	# by marker.

	mrkCol = Gatherer.columnNumber (cols, 'marker_key')
	regMrkCol = Gatherer.columnNumber (cols, 'interacting_marker_key')
	cols.append ('in_teaser')

	lastMarkerKey = None
	teasedKeys = []		# reg marker keys in teaser for current marker

	for row in rows:
		markerKey = row[mrkCol]
		regMarkerKey = row[regMrkCol]

		inTeaser = 0

		if lastMarkerKey != markerKey:
			lastMarkerKey = markerKey
			teasedKeys = [ regMarkerKey ]
			inTeaser = 1

		elif len(teasedKeys) < 3:
			if regMarkerKey not in teasedKeys:
				teasedKeys.append(regMarkerKey)
				inTeaser = 1

		row.append(inTeaser)

	return cols, rows

###--- Classes ---###

class RegGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the Template table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for Templates,
	#	collates results, writes tab-delimited text file

	def processInteractionQuery (self, queryNumber, participant = 0):
		# process a set of results for marker-to-marker interactions.
		# 'queryNumber' is an index into self.results to identify
		# which set of results to process.  'participant' indicates if
		# these interactions are from the participant's perspective
		# (1) or from the organizer's perspective (0).

		global miGenerator

		cols, rows = self.results[queryNumber]

		# add chromosome and start coordinate fields to each row, to
		# use for sorting

		cols.append ('chromosome')
		cols.append ('startCoordinate')

		relMrkCol = Gatherer.columnNumber (cols, 'interacting_marker_key')

		rows = dbAgnostic.tuplesToLists(rows)

		for row in rows:
			row.append(getChromosome(row[relMrkCol]))
			row.append(getStartCoord(row[relMrkCol]))

		# update sorting of rows to group by marker key, relationship
		# category, and a smart alpha sort on interacting marker symbol

		mrkKeyCol = Gatherer.columnNumber (cols, 'marker_key')
		relSymbolCol = Gatherer.columnNumber (cols,
			'interacting_marker_symbol')
		categoryCol = Gatherer.columnNumber (cols,
			'relationship_category')
		termCol = Gatherer.columnNumber (cols, 'relationship_term')
		chrCol = Gatherer.columnNumber (cols, 'chromosome')
		coordCol = Gatherer.columnNumber (cols, 'startCoordinate')

		ListSorter.setSortBy ( [ (mrkKeyCol, ListSorter.NUMERIC),
			(categoryCol, ListSorter.ALPHA),
			(termCol, ListSorter.ALPHA),
			(chrCol, ListSorter.CHROMOSOME),
			(coordCol, ListSorter.NUMERIC),
			] )

		rows.sort (ListSorter.compare)
		logger.debug ('Sorted %d query %d rows' % (len(rows),
			queryNumber))

		# add mi_key field and sequence number field to each row, and
		# one to indicate that these are not reversed relationships

		relKeyCol = Gatherer.columnNumber (cols, '_Relationship_key') 
		cols.append ('mi_key')
		cols.append ('sequence_num')
		cols.append ('is_reversed')
		seqNum = 0

		for row in rows:
			row.append (miGenerator.getKey((row[relKeyCol], 
				participant)))
			seqNum = seqNum + 1 
			row.append (seqNum)
			row.append (participant)

		return addInTeaserFlag(cols, rows)

	def processQuery0 (self):
		# query 0 : basic marker-to-marker relationships

		return self.processInteractionQuery(0, 0)

	def processQuery1 (self):
		# query 1 : reversed marker-to-marker relationships

		return self.processInteractionQuery(1, 1)

	def processPropertyQuery (self, queryNumber, participant = 0):
		# process a set of results for marker-to-marker interactions'
		# properties.  'queryNumber' is an index into self.results to
		# identify which set of results to process.  'participant'
		# indicates if the relationships are from the participant's
		# perspective (1) or from the organizer's perspective (0).

		cols, rows = self.results[queryNumber]

		# add mi_key to each row

		relKeyCol = Gatherer.columnNumber (cols, '_Relationship_key')

		cols.append ('mi_key')

		rows = dbAgnostic.tuplesToLists(rows)

		for row in rows:
			row.append (miGenerator.getKey((row[relKeyCol], 
				participant)))

		# sort rows to be ordered by mi_key, property name, and
		# property value

		miKeyCol = Gatherer.columnNumber (cols, 'mi_key')
		nameCol = Gatherer.columnNumber (cols, 'name')
		valueCol = Gatherer.columnNumber (cols, 'value')

		ListSorter.setSortBy ( [
			(miKeyCol, ListSorter.NUMERIC),
			(nameCol, ListSorter.ALPHA),
			(valueCol, ListSorter.SMART_ALPHA)
			] )
		rows.sort (ListSorter.compare)
		logger.debug ('Sorted %d query %d rows' % (len(rows),
			queryNumber))

		# add sequence number to each row

		cols.append ('sequence_num')

		for row in rows:
			self.maxPropertySeqNum = self.maxPropertySeqNum + 1
			row.append (self.maxPropertySeqNum)

		return cols, rows

	def processQuery2 (self):
		# query 2 : properties for reverse marker-to-marker
		# relationships

		return self.processPropertyQuery(2, 0)

	def processQuery2Reversed (self):
		# query 2 reversed : properties for reverse marker-to-marker
		# relationships

		return self.processPropertyQuery(2, 1)

	def processNotesQuery (self, queryNumber, participant):
		cols, rows = self.results[queryNumber]

		keyCol = Gatherer.columnNumber (cols, '_Relationship_key')
		noteCol = Gatherer.columnNumber (cols, 'note')
		seqNumCol = Gatherer.columnNumber (cols, 'sequenceNum')

		notes = {}		# mi_key -> note

		for row in rows:
			miKey = miGenerator.getKey((row[keyCol], participant))

			if notes.has_key(miKey):
				notes[miKey] = notes[miKey] + row[noteCol]
			else:
				notes[miKey] = row[noteCol]

		logger.debug ('Collated %d rows into %d notes' % (len(rows),
			len(notes)) )

		# now compile the notes into properties-style rows

		miKeys = notes.keys()
		miKeys.sort()

		pCols = [ 'mi_key', 'name', 'value', 'sequence_num',
			'_relationship_key', 'sequencenum' ]			
		pRows = []

		for miKey in miKeys:
			self.maxPropertySeqNum = self.maxPropertySeqNum + 1

			# Both _Relationship_key and sequenceNum are ignored
			# from here onward, so we can just toss in zeroes to
			# ensure that we have the right number of columns to
			# match the other property rows.  (needed to merge 
			# the sets of rows together)

			row = [ miKey, 'note', notes[miKey], 
				self.maxPropertySeqNum, 0, 0 ]

			pRows.append(row)

		logger.debug ('Converted %d notes into properties' % \
			len(pRows))

		return pCols, pRows

	def processQuery3 (self):
		# query 3 : notes for marker-to-marker interactions

		return self.processNotesQuery(3, 0)

	def processQuery3Reversed (self):
		# query 3 : notes for marker-to-marker interactions

		return self.processNotesQuery(3, 1)

	def collateResults (self):

		self.maxPropertySeqNum = 0

		# interaction rows from queryies 0 and 1

		cols, rows = self.processQuery0()
		cols1, rows1 = self.processQuery1()

		cols, rows = dbAgnostic.mergeResultSets (cols, rows,
			cols1, rows1)

		logger.debug ('Found %d interaction rows' % len(rows))
		self.output.append ( (cols, rows) )

		# property rows from query 2 (and also reversed version)

		cols2, rows2 = self.processQuery2()
		cols3, rows3 = self.processQuery2Reversed()

		cols2, rows2 = dbAgnostic.mergeResultSets (cols2, rows2,
			cols3, rows3)

		logger.debug ('Found %d property rows' % len(rows2))

		# notes rows from query 3 (and also reversed version)

		cols4, rows4 = self.processQuery3()
		cols5, rows5 = self.processQuery3Reversed()

		cols2, rows2 = dbAgnostic.mergeResultSets (cols2, rows2,
			cols4, rows4)
		cols2, rows2 = dbAgnostic.mergeResultSets (cols2, rows2,
			cols5, rows5)

		logger.debug ('Found %d notes rows' % (len(rows4) + len(rows5)))

		self.output.append ( (cols2, rows2) )
		return

###--- globals ---###

cmds = [
	# 0. basic marker-to-marker relationship data
	'''select r._Relationship_key,
			c.name as relationship_category,
			r._Object_key_1 as marker_key,
			r._Object_key_2 as interacting_marker_key,
			m.symbol as interacting_marker_symbol,
			s.synonym as relationship_term,
			a.accID as interacting_marker_id,
			q.term as qualifier,
			e.abbreviation as evidence_code,
			r._Refs_key as reference_key,
			bc.accID as jnum_id
		from mgi_relationship_category c,
			mgi_relationship r,
			mrk_marker m,
			acc_accession a,
			voc_term q,
			voc_term e,
			acc_accession bc,
			mgi_synonym s,
			mgi_synonymtype st
		where c._Category_key = r._Category_key
			and c._MGIType_key_1 = 2
			and c._MGIType_key_2 = 2
			and r._Object_key_2 = m._Marker_key
			and r._RelationshipTerm_key = s._Object_key
			and m._Marker_key = a._Object_key
			and a._MGIType_key = 2
			and a._LogicalDB_key = 1
			and a.preferred = 1
			and r._Qualifier_key = q._Term_key
			and r._Evidence_key = e._Term_key
			and r._Refs_key = bc._Object_key
			and bc._MGIType_key = 1
			and bc._LogicalDB_key = 1
			and bc.preferred = 1
			and bc.prefixPart = 'J:'
			and s._SynonymType_key = st._SynonymType_key
			and st._MGIType_key = 13
			and st.synonymType = 'related organizer'
			and r._Category_key = 1001
		order by r._Object_key_1''',

	# 1. reversed marker-to-marker relationship data
	'''select r._Relationship_key,
			c.name as relationship_category,
			r._Object_key_2 as marker_key,
			r._Object_key_1 as interacting_marker_key,
			m.symbol as interacting_marker_symbol,
			s.synonym as relationship_term,
			a.accID as interacting_marker_id,
			q.term as qualifier,
			e.abbreviation as evidence_code,
			r._Refs_key as reference_key,
			bc.accID as jnum_id
		from mgi_relationship_category c,
			mgi_relationship r,
			mrk_marker m,
			acc_accession a,
			voc_term q,
			voc_term e,
			acc_accession bc,
			mgi_synonym s,
			mgi_synonymtype st
		where c._Category_key = r._Category_key
			and c._MGIType_key_1 = 2
			and c._MGIType_key_2 = 2
			and r._Object_key_1 = m._Marker_key
			and r._RelationshipTerm_key = s._Object_key
			and m._Marker_key = a._Object_key
			and a._MGIType_key = 2
			and a._LogicalDB_key = 1
			and a.preferred = 1
			and r._Qualifier_key = q._Term_key
			and r._Evidence_key = e._Term_key
			and r._Refs_key = bc._Object_key
			and bc._MGIType_key = 1
			and bc._LogicalDB_key = 1
			and bc.preferred = 1
			and bc.prefixPart = 'J:'
			and s._SynonymType_key = st._SynonymType_key
			and st._MGIType_key = 13
			and st.synonymType = 'related participant'
			and r._Category_key = 1001
		order by r._Object_key_2''',

	# 2. properties
	'''select p._Relationship_key,
			t.term as name,
			p.value,
			p.sequenceNum
		from mgi_relationship_property p,
			voc_term t,
			mgi_relationship r
		where p._Relationship_key = r._Relationship_key
			and r._Category_key = 1001
			and p._PropertyName_key = t._Term_key
		order by p._Relationship_key, p.sequenceNum''', 

	# 3. relationship notes (needed for Excel/tab downloads)
	'''select r._Relationship_key,
			c.sequenceNum,
			c.note
		from mgi_relationship r,
			mgi_notetype t,
			mgi_note n,
			mgi_notechunk c
		where r._Relationship_key = n._Object_key
			and r._Category_key = 1001
			and t._NoteType_key = n._NoteType_key
			and t._MGIType_key = 40
			and n._Note_key = c._Note_key
			order by r._Relationship_key, c.sequenceNum''',
	]

# prefix for the filename of the output file
files = [
	('marker_interaction',
		[ 'mi_key', 'marker_key', 'interacting_marker_key',
			'interacting_marker_symbol', 'interacting_marker_id',
			'relationship_category', 'relationship_term',
			'qualifier', 'evidence_code', 'reference_key',
			'jnum_id', 'sequence_num', 'in_teaser', 'is_reversed'
			],
		'marker_interaction'),

	('marker_interaction_property',
		[ Gatherer.AUTO, 'mi_key', 'name', 'value', 'sequence_num' ],
		'marker_interaction_property'),
	]

# global instance of a RegGatherer
gatherer = RegGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
