#!/usr/local/bin/python
# 
# gathers data for the 'marker_related_marker' and 'marker_mrm_property'
# tables in the front-end database

import KeyGenerator
import Gatherer
import logger
import dbAgnostic
import ListSorter

###--- Sample Data ---###

SampleData = \
"""Add	Cluster	MGI:3629589	Mir181a-1	member_of	MGI:5313566	Mirc14	not specified	UN	J:88535	wp	Sample data - small miRNA cluster
Add	Cluster	MGI:3618735	Mir181b-1	member_of	MGI:5313566	Mirc14	not specified	UN	J:88535	wp	Sample data - small mi RNA cluster
Add	Cluster	MGI:3619386	Mir379	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3619399	Mir411	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3619322	Mir299	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3619389	Mir380	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3783370	Mir1197	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3619334	Mir323	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3691604	Mir758	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3619341	Mir329	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3619426	Mir494	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3629899	Mir679	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3783366	Mir1193	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3629901	Mir666	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3619432	Mir543	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3629903	Mir495	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3629904	Mir667	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3619381	Mir376c	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3718556	Mir654	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3619378	Mir376b	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3619377	Mir376a	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3619323	Mir300	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3619391	Mir381	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3619424	Mir487b	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3619427	Mir539	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:3718547	Mir544	member_of	MGI:5049944	Mirc6	not specified	UN	J:88535	wp	Sample data - large miRNA cluster
Add	Cluster	MGI:96170	Hoxa1	member_of	MGI:96169	Hoxa	not specified	UN	J:169621	jrl	Sample data - hox cluster
Add	Cluster	MGI:96174	Hoxa2	member_of	MGI:96169	Hoxa	not specified	UN	J:169621	jrl	Sample data - hox cluster
Add	Cluster	MGI:96175	Hoxa3	member_of	MGI:96169	Hoxa	not specified	UN	J:169621	jrl	Sample data - hox cluster
Add	Cluster	MGI:96176	Hoxa4	member_of	MGI:96169	Hoxa	not specified	UN	J:169621	jrl	Sample data - hox cluster
Add	Cluster	MGI:96177	Hoxa5	member_of	MGI:96169	Hoxa	not specified	UN	J:169621	jrl	Sample data - hox cluster
Add	Cluster	MGI:96178	Hoxa6	member_of	MGI:96169	Hoxa	not specified	UN	J:169621	jrl	Sample data - hox cluster
Add	Cluster	MGI:96179	Hoxa7	member_of	MGI:96169	Hoxa	not specified	UN	J:169621	jrl	Sample data - hox cluster
Add	Cluster	MGI:96180	Hoxa9	member_of	MGI:96169	Hoxa	not specified	UN	J:169621	jrl	Sample data - hox cluster
Add	Cluster	MGI:96171	Hoxa10	member_of	MGI:96169	Hoxa	not specified	UN	J:169621	jrl	Sample data - hox cluster
Add	Cluster	MGI:96172	Hoxa11	member_of	MGI:96169	Hoxa	not specified	UN	J:169621	jrl	Sample data - hox cluster
Add	Cluster	MGI:96173	Hoxa13	member_of	MGI:96169	Hoxa	not specified	UN	J:169621	jrl	Sample data - hox cluster
Add	Cluster	MGI:96478	Igh-V10	member_of	MGI:96442	Igh	not specified	UN	J:5559	jrl	Igh cluster that's a member_of another cluster
Add	Cluster	MGI:4439620	Ighv10-1	member_of 	MGI:96442	Igh	not specified	UN	J:5559	jrl	markers that are members of 2 clusters
Add	Cluster	MGI:5009903	Ighv10-2	member_of 	MGI:96442	Igh	not specified	UN	J:5559	jrl	markers that are members of 2 clusters
Add	Cluster	MGI:3648785	Ighv10-3	member_of 	MGI:96442	Igh	not specified	UN	J:5559	jrl	markers that are members of 2 clusters
Add	Cluster	MGI:5009905	Ighv10-4	member_of 	MGI:96442	Igh	not specified	UN	J:5559	jrl	markers that are members of 2 clusters
Add	Cluster	MGI:96449	Igh-7	member_of 	MGI:96442	Igh	not specified	UN	J:5559	jrl	Igh member w/coords
Add	Cluster	MGI:4439620	Ighv10-1	member_of 	MGI:96478	Igh-V10	not specified	UN	J:5559	jrl	markers that are members of 2 clusters
Add	Cluster	MGI:5009903	Ighv10-2	member_of 	MGI:96478	Igh-V10	not specified	UN	J:5559	jrl	markers that are members of 2 clusters
Add	Cluster	MGI:3648785	Ighv10-3	member_of 	MGI:96478	Igh-V10	not specified	UN	J:5559	jrl	markers that are members of 2 clusters
Add	Cluster	MGI:5009905	Ighv10-4	member_of 	MGI:96478	Igh-V10	not specified	UN	J:5559	jrl	markers that are members of 2 clusters
"""

###--- Globals ---###

mrmGenerator = KeyGenerator.KeyGenerator('marker_related_marker')

###--- Functions ---###

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
	
def slicedData():
	# provides SampleData, broken down into rows and columns.
	# replaces 'Add' column with row number; adds columns for marker keys
	# and reference key.

	rows = []
	for row in SampleData.split('\n'):
		if (row.strip() == ''):
			continue

		rows.append (row.split('\t'))
		rows[-1][0] = str(len(rows))

		rows[-1].append (keyLookup (rows[-1][5], 2))
		rows[-1].append (keyLookup (rows[-1][2], 2))
		rows[-1].append (keyLookup (rows[-1][9], 1))

	return rows

def extractColumns (row, columnNumbers):
	r = []
	for c in columnNumbers:
		r.append(row[c])
	return r

def emulateQuery0():
	# parse SampleData and give results as if data came from query 0

	cols = [ '_Relationship_key', 'relationship_category', 'marker_key',
		'related_marker_key', 'related_marker_symbol',
		'related_marker_id', 'relationship_term', 'qualifier',
		'evidence_code', 'reference_key', 'jnum_id' ]

	rows = []
	for row in slicedData():
		rows.append (extractColumns (row,
			[ 0, 1, 12, 13, 3, 2, 4, 7, 8, 14, 9 ]) )
	return cols, rows

def emulateQuery1():
	# parse SampleData and give results as if data came from query 1

	cols = [ '_Relationship_key', 'relationship_category', 'marker_key',
		'related_marker_key', 'related_marker_symbol',
		'related_marker_id', 'relationship_term', 'qualifier',
		'evidence_code', 'reference_key', 'jnum_id' ]

	rows = []
	for row in slicedData():
		rows.append (extractColumns (row,
			[ 0, 1, 13, 12, 6, 5, 4, 7, 8, 14, 9 ]) )

	for row in rows:
		if row[6] == 'member_of':
			row[6] = 'has_member'
		elif row[6] == 'member of':
			row[6] = 'has member'

	return cols, rows

def emulateQuery2():
	# parse SampleData and give results as if data came from query 2

	rows = []
	for row in slicedData():
		rows.append (extractColumns (row, [ 0, 11 ]))
		rows[-1].append ('note')
		rows[-1].append (len(rows))

	cols = [ '_Relationship_key', 'value', 'name' ]

	return cols, rows

###--- Classes ---###

class MrmGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the Template table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for Templates,
	#	collates results, writes tab-delimited text file

	def mrmKey (self, relationshipKey, reverse = False):
		key = (relationshipKey, reverse)

		if not self.mrmMap.has_key(key):
			self.mrmMap[key] = len(self.mrmMap) + 1

		return self.mrmMap[key]

	def processQuery0 (self):
		# query 0 : basic marker-to-marker relationships

		global mrmGenerator

		cols, rows = self.results[0]

		relKeyCol = Gatherer.columnNumber (cols, '_Relationship_key')
		seqNum = 0

		cols.append ('mrm_key')
		cols.append ('sequence_num')

		# update sorting of rows to group by marker key, relationship
		# category, and a smart alpha sort on related marker symbol

		mrkKeyCol = Gatherer.columnNumber (cols, 'marker_key')
		relSymbolCol = Gatherer.columnNumber (cols,
			'related_marker_symbol')
		categoryCol = Gatherer.columnNumber (cols,
			'relationship_category')
		termCol = Gatherer.columnNumber (cols, 'relationship_term')

		ListSorter.setSortBy ( [ (mrkKeyCol, ListSorter.NUMERIC),
			(categoryCol, ListSorter.ALPHA),
			(termCol, ListSorter.ALPHA),
			(relSymbolCol, ListSorter.SMART_ALPHA) ] )

		rows.sort (ListSorter.compare)
		logger.debug ('Sorted %d query 0 rows' % len(rows))

		# add mrm_key field and sequence number field to each row

		for row in rows:
			row.append (mrmGenerator.getKey((row[relKeyCol], 0)))
			seqNum = seqNum + 1 
			row.append (seqNum)

		return cols, rows

	def processQuery1 (self, query1Cols):
		# query 1 : reversed marker-to-marker relationships

		cols1, rows1 = self.results[1]

		# assume cols == cols1; if not, we need to map them (to do)

		for c in cols1:
			if (cols1.index(c) != query1Cols.index(c)):
				raise 'error', 'List indexes differ'

		# update sorting of rows to group by marker key, relationship
		# category, and a smart alpha sort on related marker symbol

		mrkKeyCol = Gatherer.columnNumber (cols1, 'marker_key')
		relSymbolCol = Gatherer.columnNumber (cols1,
			'related_marker_symbol')
		categoryCol = Gatherer.columnNumber (cols1,
			'relationship_category')
		termCol = Gatherer.columnNumber (cols1, 'relationship_term')

		ListSorter.setSortBy ( [ (mrkKeyCol, ListSorter.NUMERIC),
			(categoryCol, ListSorter.ALPHA),
			(termCol, ListSorter.ALPHA),
			(relSymbolCol, ListSorter.SMART_ALPHA) ] )

		rows1.sort (ListSorter.compare)
		logger.debug ('Sorted %d query 1 rows' % len(rows1))

		# add mrm_key field and sequence number field to each row

		cols1.append ('mrm_key')
		cols1.append ('sequence_num')

		relKeyCol = Gatherer.columnNumber (cols1, '_Relationship_key')

		seqNum = 0
		for row in rows1:
			row.append (mrmGenerator.getKey((row[relKeyCol], 1)))

			seqNum = seqNum + 1
			row.append (seqNum)

		return cols1, rows1

	def processQuery2 (self):
		# query 2 : properties for marker-to-marker relationships

		cols, rows = self.results[2]

		# add mrm_key to each row

		relKeyCol = Gatherer.columnNumber (cols, '_Relationship_key')

		cols.append ('mrm_key')

		for row in rows:
			row.append (mrmGenerator.getKey((row[relKeyCol], 0)))

		# sort rows to be ordered by mrm_key, property name, and
		# property value

		mrmKeyCol = Gatherer.columnNumber (cols, 'mrm_key')
		nameCol = Gatherer.columnNumber (cols, 'name')
		valueCol = Gatherer.columnNumber (cols, 'value')

		ListSorter.setSortBy ( [
			(mrmKeyCol, ListSorter.NUMERIC),
			(nameCol, ListSorter.ALPHA),
			(valueCol, ListSorter.SMART_ALPHA)
			] )
		rows.sort (ListSorter.compare)
		logger.debug ('Sorted %d query 2 rows' % len(rows))

		# add sequence number to each row

		cols.append ('sequence_num')

		seqNum = 0
		for row in rows:
			seqNum = seqNum + 1
			row.append (seqNum)

		return cols, rows

	def processQuery3 (self):
		return [], []

	def processQuery4 (self):
		return [], []

	def processQuery5 (self):
		return [], []

	def collateResults (self):
		self.results = [ emulateQuery0(), emulateQuery1(),
			emulateQuery2() ]

		self.mrmMap = {}	# maps _Relationship_key to mrm_key

		cols, rows = self.processQuery0()
		cols1, rows1 = self.processQuery1(cols)

		logger.debug ('Found %d rows for queries 0-1' % (
			len(rows) + len(rows1)) )
		self.output.append ( (cols, rows + rows1) )

		cols, rows = self.processQuery2()

		# query 3 : properties for reversed mrk-to-mrk relationships

		# query 4 : notes for mrk-to-mrk relationships

		# query 5 : notes for reversed mrk-to-mrk relationships

		logger.debug ('Found %d rows for queries 2-5' % len(rows))
		self.output.append ( (cols, rows) )

		# fake data to get extra tables created temporarily

		self.output.append ( (self.files[-2][1][1:], []) )
		self.output.append ( (self.files[-1][1][1:], []) )
		return

###--- globals ---###

cmds = []

cmds2 = [
	# 0. basic marker-to-marker relationship data
	'''select r._Relationship_key,
			c.name as relationship_category,
			r._Object_key_1 as marker_key,
			r._Object_key_2 as related_marker_key,
			m.symbol as related_marker_symbol,
			t.term as relationship_term,
			a.accID as related_marker_id,
			q.term as qualifier,
			e.term as evidence_code,
			bc._Refs_key as reference_key,
			bc.jnum_id
		from mgi_relationship_category c,
			mgi_relationship r,
			mrk_marker m,
			voc_term t,
			acc_accession a,
			voc_term q,
			voc_term e,
			bib_citation_cache bc
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
		order by r._Object_key_1''',

	# 1. reversed marker-to-marker relationship data
	'''TBD
		''',

	# 2. properties
	'''select _Relationship_key,
			name,
			value,
			sequenceNum
		from mgi_relationship_property
		order by _Relationship_key, sequenceNum''', 

	# 3. properties for reverse relationships
	'''TBD
		''',

	# 4. relationship notes (if needed for display)
	'''TBD
		''',

	# 5. relationship notes (if needed for display)
	'''TBD
		''',

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

	# These last two should move to a separate gatherer, once we have
	# sample data.  They are here only for convenience in initial
	# implementation.

	('marker_related_allele',
		[ Gatherer.AUTO, 'marker_key', 'related_allele_key', 
		'related_allele_symbol', 'related_allele_id',
		'relationships_category', 'relationship_term', 'qualifier',
		'evidence_code', 'reference_key', 'jnum_id', 'sequence_num' ],
		'marker_related_allele'),

	('allele_related_marker',
		[ Gatherer.AUTO, 'allele_key', 'related_marker_key', 
		'related_marker_symbol', 'related_marker_id',
		'relationships_category', 'relationship_term', 'qualifier',
		'evidence_code', 'reference_key', 'jnum_id', 'sequence_num' ],
		'allele_related_marker'),
	]

# global instance of a MrmGatherer
gatherer = MrmGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
