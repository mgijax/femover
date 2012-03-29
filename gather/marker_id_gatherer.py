#!/usr/local/bin/python
# 
# gathers data for the 'markerID' table in the front-end database

import Gatherer
import logger

###--- Globals ---###

EXCLUDE_FROM_OTHER_DBS = [ 1, 9, 13, 27, 28, 41, 59, 81, 86, 88,
		95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 122, 123,
		131, 132, 133, 134, 135, 139, 140, 141, 144 ]

LDB_ORDER = [ 85, 60, 55, 23, 35, 36, 53, 8, 45, 126 ]

###--- Functions ---###

ldbKeyCol = None
ldbNameCol = None
idCol = None

def ldbCompare (a, b):
	aKey = a[ldbKeyCol]
	bKey = b[ldbKeyCol]

	# compare based on the defined LDB_ORDER where applicable

	if aKey in LDB_ORDER:
		if bKey in LDB_ORDER:
			return cmp(LDB_ORDER.index(aKey),
				LDB_ORDER.index(bKey))
		return -1
	
	if bKey in LDB_ORDER:
		return 1

	# neither row's logical db is in LDB_ORDER, so sort by logical db name

	i = cmp(a[ldbNameCol], b[ldbNameCol])

	if i == 0:
		# if the logical databases matched, just sort by ID

		return cmp(a[idCol], b[idCol])
	return i

markerIdKeys = {}	# (marker key, logical db, ID) -> marker ID key
def getMarkerIdKey (markerKey, ldbKey, accID):
	# the unique ID for this record for the marker ID table

	global markerIdKeys

	key = (markerKey, ldbKey, accID)
	if not markerIdKeys.has_key(key):
		markerIdKeys[key] = len(markerIdKeys) + 1
	return markerIdKeys[key] 

###--- Classes ---###

class MarkerIDGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the markerID table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for marker IDs,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		# slice and dice the query results to produce our set of
		# final results

		global ldbKeyCol, ldbNameCol, idCol

		# first, gather all the IDs by marker

		ids = {}	# marker key -> ID rows
		cols, rows = self.results[0]

		ldbKeyCol = Gatherer.columnNumber (cols, '_LogicalDB_key')
		ldbNameCol = Gatherer.columnNumber (cols, 'logicalDB')
		idCol = Gatherer.columnNumber (cols, 'accID')
		keyCol = Gatherer.columnNumber (cols, 'markerKey')
		preferredCol = Gatherer.columnNumber (cols, 'preferred')
		privateCol = Gatherer.columnNumber (cols, 'private')

		for row in rows:
			key = row[keyCol]
			if ids.has_key(key):
				ids[key].append (row)
			else:
				ids[key] = [ row ]

		logger.debug ('Collected IDs for %d markers' % len(ids))

		markerKeys = ids.keys()
		markerKeys.sort()

		logger.debug ('Sorted %d marker keys' % len(markerKeys))

		for key in markerKeys:
			ids[key].sort(ldbCompare)

		logger.debug ('Sorted IDs for %d marker keys' % len(ids))

		# now compile our IDs into our set of final results

		idResults = []
		idColumns = [ 'markerIdKey', 'markerKey', 'logicalDB',
			'accID', 'preferred', 'private',
			'isForOtherDbSection', 'sequence_num' ]

		i = 0
		seenIdKeys = {}

		for key in markerKeys:
			for r in ids[key]:
				markerIdKey = getMarkerIdKey (key,
					r[ldbKeyCol], r[idCol])

				# ensure we have no duplicate IDs (skip any
				# duplicates)

				if seenIdKeys.has_key(markerIdKey):
					continue
				seenIdKeys[markerIdKey] = 1

				i = i + 1
				if r[ldbKeyCol] in EXCLUDE_FROM_OTHER_DBS:
					otherDB = 0
				elif r[privateCol] == 1:
					otherDB = 0
				else:
					otherDB = 1

				idResults.append ( [ markerIdKey,
					key,
					r[ldbNameCol],
					r[idCol],
					r[preferredCol],
					r[privateCol],
					otherDB,
					i
					] )
		self.output.append ( (idColumns, idResults) ) 
		logger.debug ('Got %d marker ID rows' % len(idResults))

		# handle query 1 -- shared gene model IDs

		otherResults = []
		otherColumns = [ 'markerIdKey', 'markerKey', 'symbol', 'accID'
			]

		cols, rows = self.results[1]

		idCol = Gatherer.columnNumber (cols, 'accID')
		ldbCol = Gatherer.columnNumber (cols, '_LogicalDB_key')
		keyCol = Gatherer.columnNumber (cols, '_Marker_key')
		baseMarkerKeyCol = Gatherer.columnNumber (cols,
			'baseMarkerKey')
		symbolCol = Gatherer.columnNumber (cols, 'symbol')
		markerIDCol = Gatherer.columnNumber (cols, 'markerID')

		for row in rows:
			markerIdKey = getMarkerIdKey (row[baseMarkerKeyCol],
				row[ldbCol], row[idCol])

			otherResults.append ([ markerIdKey,
				row[keyCol], row[symbolCol], row[markerIDCol]
				] )

		self.output.append ( (otherColumns, otherResults) )
		logger.debug ('Got %d shared GM ID rows' % len(otherResults)) 
		return

###--- globals ---###

cmds = [
	# 0. all IDs for each marker
	'''select a._Object_key as markerKey, a._LogicalDB_key,
		a.accID, a.preferred, a.private, ldb.name as logicalDB
	from acc_accession a, acc_logicaldb ldb
	where a._MGIType_key = 2
		and exists (select 1 from mrk_marker m
			where a._Object_key = m._Marker_key)
		and a._LogicalDB_key = ldb._LogicalDB_key''',

	# 1. some gene model IDs are shared by multiple markers; we need to
	# look up the markers sharing each gene model ID
	'''select a.accID, a._LogicalDB_key, m._Marker_key, m.symbol,
		c.accID as markerID, a._Object_key as baseMarkerKey
	from acc_accession a, acc_accession b, mrk_marker m, acc_accession c
	where a._LogicalDB_key in (85, 60, 55)
		and a.accID = b.accID
		and a._LogicalDB_key = b._LogicalDB_key
		and a._MGIType_key = 2
		and b._MGIType_key = 2
		and a._Object_key != b._Object_key
		and b._Object_key = m._Marker_key
		and m._Marker_key = c._Object_key
		and exists (select 1 from mrk_marker m2
			where m2._Marker_key = a._Object_key)
		and c._MGIType_key = 2
		and c._LogicalDB_key = 1
		and c.preferred = 1''',
	]

# order of fields (from the query results) to be written to the
# output file
files = [ ('marker_id',
		[ 'markerIdKey', 'markerKey', 'logicalDB', 'accID',
			'preferred', 'private', 'isForOtherDbSection',
			'sequence_num' ],
		'marker_id'),

	('marker_id_other_marker',
		[ Gatherer.AUTO, 'markerIdKey', 'markerKey', 'symbol',
			'accID' ],
		'marker_id_other_marker')
	]

# global instance of a markerIDGatherer
gatherer = MarkerIDGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
