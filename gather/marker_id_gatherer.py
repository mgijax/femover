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

###--- Classes ---###

class MarkerIDGatherer (Gatherer.Gatherer):
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

		self.finalResults = []
		self.finalColumns = [ 'markerKey', 'logicalDB', 'accID',
			'preferred', 'private', 'isForOtherDbSection',
			'sequence_num' ]

		i = 0
		for key in markerKeys:
			for r in ids[key]:
				i = i + 1
				if r[ldbKeyCol] in EXCLUDE_FROM_OTHER_DBS:
					otherDB = 0
				elif r[privateCol] == 1:
					otherDB = 0
				else:
					otherDB = 1

				self.finalResults.append ( [ key,
					r[ldbNameCol],
					r[idCol],
					r[preferredCol],
					r[privateCol],
					otherDB,
					i
					] )
		return

###--- globals ---###

cmds = [
	'''select a._Object_key as markerKey, a._LogicalDB_key,
		a.accID, a.preferred, a.private, ldb.name as logicalDB
	from acc_accession a, acc_logicaldb ldb
	where a._MGIType_key = 2
		and a._LogicalDB_key = ldb._LogicalDB_key'''
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'markerKey', 'logicalDB', 'accID', 'preferred',
	'private', 'isForOtherDbSection', 'sequence_num' ]

# prefix for the filename of the output file
filenamePrefix = 'marker_id'

# global instance of a markerIDGatherer
gatherer = MarkerIDGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
