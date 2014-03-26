#!/usr/local/bin/python
# 
# gathers data for the 'marker_related_marker' and 'marker_reg_property'
# tables in the front-end database

import KeyGenerator
import Gatherer
import logger
import types
import dbAgnostic
import ListSorter

###--- Globals ---###

regGenerator = KeyGenerator.KeyGenerator('marker_related_marker')

###--- Functions ---###

# marker key -> (genetic chrom, genomic chrom, start coord, end coord)
coordCache = {}	

def populateCache():
	# populate the global 'coordCache' with location data for markers

	global coordCache

	cmd = '''select _Marker_key, genomicChromosome, chromosome,
			startCoordinate, endCoordinate
		from mrk_location_cache
		where _Organism_key = 1'''

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

def getMarkerCoords(marker):
	# get (genetic chrom, genomic chrom, start coord, end coord) for the
	# given marker key or ID

	if len(coordCache) == 0:
		populateCache()

	if type(marker) == types.StringType:
		markerKey = keyLookup(marker, 2)
	else:
		markerKey = marker

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

keyCache = {}

def keyLookup (accID, mgiType):
	key = (accID, mgiType)

	if not keyCache.has_key(key):
		cmd = '''select _Object_key
			from acc_accession
			where accID = '%s'
				and _MGIType_key = %d''' % (accID, mgiType)

		(cols, rows) = dbAgnostic.execute(cmd)

		if not rows:
			keyCache[key] = None 
		else:
			keyCache[key] = rows[0][0]

	return keyCache[key]
	
def mergeResultSets (cols1, rows1, cols2, rows2):
	# return a unified (cols, rows) pair, accounting for the fact
	# that column ordering in cols1 and cols2 may be different.
	# Assumes that every the column names in cols1 and cols2 are
	# the same names, even if they appear in a different order.
	# Note: This function can alter rows1.

	colsMatch = True

	for c in cols1:
		if (cols1.index(c) != cols2.index(c)):
			colsMatch = False
			break

	# easy case: the columns are already in the same order, so we
	# can just concatenate the lists

	if colsMatch:
		return (cols1, rows1 + rows2)

	# Otherwise, we'll need to ensure we re-order the values in
	# rows2 to match and append those rows to rows1.

	colOrder = []
	for c in cols1:
		colOrder.append(cols2.index(c))

	for row in rows2:
		r = []
		for c in colOrder:
			r.append(row[c])
		rows1.append(r)

	return (cols1, rows1)

def tuplesToLists (rows):
	# 'rows' is a list of query results.  If these results are tuples,
	# then convert them to lists instead.

	if len(rows) == 0:
		return rows

	if type(rows[0]) == types.ListType:
		return rows

	r = []
	for row in rows:
		r.append(list(row))
	return r

###--- Classes ---###

class MrmGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the Template table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for Templates,
	#	collates results, writes tab-delimited text file

	def processRelationshipQuery (self, queryNumber, participant = 0):
		# process a set of results for marker-to-marker relationships.
		# 'queryNumber' is an index into self.results to identify
		# which set of results to process.  'participant' indicates if
		# these relationships are from the participant's perspective
		# (1) or from the organizer's perspective (0).

		global regGenerator

		cols, rows = self.results[queryNumber]

		# add chromosome and start coordinate fields to each row

		cols.append ('chromosome')
		cols.append ('startCoordinate')

		relMrkCol = Gatherer.columnNumber (cols, 'related_marker_key')

		rows = tuplesToLists(rows)

		for row in rows:
			row.append(getChromosome(row[relMrkCol]))
			row.append(getStartCoord(row[relMrkCol]))

		# update sorting of rows to group by marker key, relationship
		# category, and a smart alpha sort on related marker symbol

		mrkKeyCol = Gatherer.columnNumber (cols, 'marker_key')
		relSymbolCol = Gatherer.columnNumber (cols,
			'related_marker_symbol')
		categoryCol = Gatherer.columnNumber (cols,
			'relationship_category')
		termCol = Gatherer.columnNumber (cols, 'relationship_term')
		chrCol = Gatherer.columnNumber (cols, 'chromosome')
		coordCol = Gatherer.columnNumber (cols, 'startCoordinate')

		ListSorter.setSortBy ( [ (mrkKeyCol, ListSorter.NUMERIC),
			(categoryCol, ListSorter.ALPHA),
			(termCol, ListSorter.ALPHA),
			(chrCol, ListSorter.CHROMOSOME),
			(coordCol, ListSorter.NUMERIC) ] )

		rows.sort (ListSorter.compare)
		logger.debug ('Sorted %d query %d rows' % (len(rows),
			queryNumber))

		# add mrm_key field and sequence number field to each row

		relKeyCol = Gatherer.columnNumber (cols, '_Relationship_key') 
		cols.append ('mrm_key')
		cols.append ('sequence_num')
		seqNum = 0

		for row in rows:
			row.append (regGenerator.getKey((row[relKeyCol], 
				participant)))
			seqNum = seqNum + 1 
			row.append (seqNum)

		return cols, rows

	def processQuery0 (self):
		# query 0 : basic marker-to-marker relationships

		return self.processRelationshipQuery(0, 0)

	def processQuery1 (self, query1Cols):
		# query 1 : reversed marker-to-marker relationships

		return self.processRelationshipQuery(1, 1)

	def processPropertyQuery (self, queryNumber, participant = 0):
		# process a set of results for marker-to-marker relationships'
		# properties.  'queryNumber' is an index into self.results to
		# identify which set of results to process.  'participant'
		# indicates if the relationships are from the participant's
		# perspective (1) or from the organizer's perspective (0).

		global regGenerator

		cols, rows = self.results[queryNumber]

		# add mrm_key to each row

		relKeyCol = Gatherer.columnNumber (cols, '_Relationship_key')

		cols.append ('mrm_key')

		rows = tuplesToLists(rows)

		for row in rows:
			row.append (regGenerator.getKey((row[relKeyCol], 
				participant)))

		# sort rows to be ordered by mrm_key, property name, and
		# property value

		regKeyCol = Gatherer.columnNumber (cols, 'mrm_key')
		nameCol = Gatherer.columnNumber (cols, 'name')
		valueCol = Gatherer.columnNumber (cols, 'value')

		ListSorter.setSortBy ( [
			(regKeyCol, ListSorter.NUMERIC),
			(nameCol, ListSorter.ALPHA),
			(valueCol, ListSorter.SMART_ALPHA)
			] )
		rows.sort (ListSorter.compare)
		logger.debug ('Sorted %d query %d rows' % (len(rows),
			queryNumber))

		# add sequence number to each row

		cols.append ('sequence_num')

		seqNum = 0
		for row in rows:
			seqNum = seqNum + 1
			row.append (seqNum)

		return cols, rows

	def processQuery2 (self):
		# query 2 : properties for marker-to-marker relationships

		return self.processPropertyQuery(2, 0)

	def processQuery3 (self):
		# query 3 : properties for reversed marker-to-marker
		# relationships

		return self.processPropertyQuery(3, 1)

	def collateResults (self):
		
		# relationship rows from queries 0 and 1

		cols, rows = self.processQuery0()
		cols1, rows1 = self.processQuery1(cols)

		cols, rows = mergeResultSets (cols, rows, cols1, rows1)

		logger.debug ('Found %d relationship rows' % len(rows))
		self.output.append ( (cols, rows) )

		# properties rows from queries 2 and 3

		cols2, rows2 = self.processQuery2()
		cols3, rows3 = self.processQuery3()

		cols2, rows2 = mergeResultSets (cols2, rows2, cols3, rows3)

		logger.debug ('Found %d property rows' % len(rows2))
		self.output.append ( (cols2, rows2) )

		# not yet needed:
		# query 4 : notes for mrk-to-mrk relationships
		# query 5 : notes for reversed mrk-to-mrk relationships

		return

###--- globals ---###

cmds = [
	# 0. basic marker-to-marker relationship data
	'''select r._Relationship_key,
			c.name as relationship_category,
			r._Object_key_1 as marker_key,
			r._Object_key_2 as related_marker_key,
			m.symbol as related_marker_symbol,
			a.accID as related_marker_id,
			q.term as qualifier,
			e.abbreviation as evidence_code,
			bc._Refs_key as reference_key,
			bc.jnumID as jnum_id,
			s.synonym as relationship_term
		from mgi_relationship_category c,
			mgi_relationship r,
			mrk_marker m,
			voc_term t,
			acc_accession a,
			voc_term q,
			voc_term e,
			bib_citation_cache bc,
			mgi_synonym s,
			mgi_synonymtype st
		where c._Category_key = r._Category_key
			and c._MGIType_key_1 = 2
			and c._MGIType_key_2 = 2
			and r._Object_key_2 = m._Marker_key
			and r._RelationshipTerm_key = t._Term_key
			and m._Marker_key = a._Object_key
			and a._MGIType_key = 2
			and a._LogicalDB_key = 1
			and a.preferred = 1
			and r._Qualifier_key = q._Term_key
			and r._Evidence_key = e._Term_key
			and r._Refs_key = bc._Refs_key
			and t._Term_key = s._Object_key
			and s._SynonymType_key = st._SynonymType_key
			and st._MGIType_key = 13
			and st.synonymType = 'related organizer'
		order by r._Object_key_1''',

	# 1. reversed marker-to-marker relationship data
	'''select r._Relationship_key,
			c.name as relationship_category,
			r._Object_key_2 as marker_key,
			r._Object_key_1 as related_marker_key,
			m.symbol as related_marker_symbol,
			a.accID as related_marker_id,
			q.term as qualifier,
			e.abbreviation as evidence_code,
			bc._Refs_key as reference_key,
			bc.jnumID as jnum_id,
			s.synonym as relationship_term
		from mgi_relationship_category c,
			mgi_relationship r,
			mrk_marker m,
			voc_term t,
			acc_accession a,
			voc_term q,
			voc_term e,
			bib_citation_cache bc,
			mgi_synonym s,
			mgi_synonymtype st
		where c._Category_key = r._Category_key
			and c._MGIType_key_1 = 2
			and c._MGIType_key_2 = 2
			and r._Object_key_1 = m._Marker_key
			and r._RelationshipTerm_key = t._Term_key
			and m._Marker_key = a._Object_key
			and a._MGIType_key = 2
			and a._LogicalDB_key = 1
			and a.preferred = 1
			and r._Qualifier_key = q._Term_key
			and r._Evidence_key = e._Term_key
			and r._Refs_key = bc._Refs_key
			and t._Term_key = s._Object_key
			and s._SynonymType_key = st._SynonymType_key
			and st._MGIType_key = 13
			and st.synonymType = 'related participant'
		order by r._Object_key_2''',

	# 2. properties
	'''select _Relationship_key,
			name,
			value,
			sequenceNum
		from mgi_relationship_property
		order by _Relationship_key, sequenceNum''', 

	# 3. properties for reverse relationships
	'''select _Relationship_key,
			name,
			value,
			sequenceNum
		from mgi_relationship_property
		order by _Relationship_key, sequenceNum''', 

	# 4. relationship notes (if needed for display)
	#'''TBD''',

	# 5. relationship notes (if needed for display)
	#'''TBD''',

	]

# prefix for the filename of the output file
files = [
	('marker_related_marker',
		[ 'mrm_key', 'marker_key', 'related_marker_key',
			'related_marker_symbol', 'related_marker_id',
			'relationship_category', 'relationship_term',
			'qualifier', 'evidence_code', 'reference_key',
			'jnum_id', 'sequence_num', ],
		'marker_related_marker'),

	('marker_mrm_property',
		[ Gatherer.AUTO, 'mrm_key', 'name', 'value', 'sequence_num' ],
		'marker_mrm_property'),
	]

# global instance of a MrmGatherer
gatherer = MrmGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
