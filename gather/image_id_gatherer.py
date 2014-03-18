#!/usr/local/bin/python
# 
# gathers data for the 'image_id' table in the front-end database
#
# 03/18/2014	lec
#	 - TR11603/exclude link to BGEM (159)

import Gatherer
import logger

###--- Globals ---###

EXCLUDE_FROM_OTHER_DBS = [ 1, 19, 159 ]
LDB_ORDER = [ 148, 105 ]		# Eurexpress, then GenePaint

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

imageIdKeys = {}	# (image key, logical db, ID) -> image ID key
def getImageIdKey (imageKey, ldbKey, accID):
	# the unique ID for this record for the image ID table

	global imageIdKeys

	key = (imageKey, ldbKey, accID)
	if not imageIdKeys.has_key(key):
		imageIdKeys[key] = len(imageIdKeys) + 1
	return imageIdKeys[key] 

###--- Classes ---###

class ImageIDGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the image ID table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for image IDs,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		# slice and dice the query results to produce our set of
		# final results

		global ldbKeyCol, ldbNameCol, idCol

		# first, gather all the IDs by image

		ids = {}	# image key -> ID rows
		cols, rows = self.results[0]

		ldbKeyCol = Gatherer.columnNumber (cols, '_LogicalDB_key')
		ldbNameCol = Gatherer.columnNumber (cols, 'logicalDB')
		idCol = Gatherer.columnNumber (cols, 'accID')
		keyCol = Gatherer.columnNumber (cols, 'imageKey')
		preferredCol = Gatherer.columnNumber (cols, 'preferred')
		privateCol = Gatherer.columnNumber (cols, 'private')

		for row in rows:
			key = row[keyCol]
			if ids.has_key(key):
				ids[key].append (row)
			else:
				ids[key] = [ row ]

		logger.debug ('Collected IDs for %d images' % len(ids))

		imageKeys = ids.keys()
		imageKeys.sort()

		logger.debug ('Sorted %d image keys' % len(imageKeys))

		for key in imageKeys:
			ids[key].sort(ldbCompare)

		logger.debug ('Sorted IDs for %d image keys' % len(ids))

		# now compile our IDs into our set of final results

		idResults = []
		idColumns = [ 'imageIdKey', 'imageKey', 'logicalDB',
			'accID', 'preferred', 'private',
			'isForOtherDbSection', 'sequence_num' ]

		i = 0
		seenIdKeys = {}

		for key in imageKeys:
			for r in ids[key]:
				imageIdKey = getImageIdKey (key,
					r[ldbKeyCol], r[idCol])

				# ensure we have no duplicate IDs (skip any
				# duplicates)

				if seenIdKeys.has_key(imageIdKey):
					continue
				seenIdKeys[imageIdKey] = 1

				i = i + 1
				if r[ldbKeyCol] in EXCLUDE_FROM_OTHER_DBS:
					otherDB = 0
				elif r[privateCol] == 1:
					otherDB = 0
				else:
					otherDB = 1

				idResults.append ( [ imageIdKey,
					key,
					r[ldbNameCol],
					r[idCol],
					r[preferredCol],
					r[privateCol],
					otherDB,
					i
					] )

		self.finalColumns = idColumns
		self.finalResults = idResults
		logger.debug ('Got %d image ID rows' % len(idResults))
		return

###--- globals ---###

cmds = [
	# 0. all IDs for each image
	'''select a._Object_key as imageKey, a._LogicalDB_key,
		a.accID, a.preferred, a.private, ldb.name as logicalDB
	from acc_accession a, acc_logicaldb ldb
	where a._MGIType_key = 9
		and exists (select 1 from img_image i
			where i._Image_key = a._Object_key)
		and a._LogicalDB_key = ldb._LogicalDB_key''',
	]

fieldOrder = [ 'imageIdKey', 'imageKey', 'logicalDB', 'accID',
			'preferred', 'private', 'isForOtherDbSection',
			'sequence_num' ]

filenamePrefix = 'image_id'

# global instance of a ImageIDGatherer
gatherer = ImageIDGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
