#!/usr/local/bin/python
# 
# gathers data for the 'marker_link' table in the front-end database

import config
import Gatherer
import logger

###--- Constants ---###

MGI = 1				# logical db
OMIM = 15
ENTREZ_GENE = 55
HGNC = 64
ENSEMBL_GENE_MODEL = 60

# preferred ordering for human/mouse links, by logical database
LDB_ORDERING = [ HGNC, MGI, ENTREZ_GENE, OMIM, ENSEMBL_GENE_MODEL ]

MOUSE = 1			# organism
HUMAN = 2

MARKER = 2			# MGI Type

HOMOLOGY_CLUSTER = 9272150	# marker cluster type (vocab term)

HOMOLOGY_LINK_GROUP = 'homology gene links'	# link group name

NO_MARKUPS = 0
HAS_MARKUPS = 1

NO_NEW_WINDOW = 0
USE_NEW_WINDOW = 1

VISTA_POINT = 'VISTA-Point'
GENE_TREE = 'Gene Tree'

###--- Functions ---###

def compareMarkerLdb (a, b):
	# sort first by marker key, then by a custom sort for ldb key

	m = cmp(a[0], b[0])		# by marker key
	if m != 0:
		return m

	aLdb = a[1]
	bLdb = b[1]

	if aLdb in LDB_ORDERING:
		if bLdb in LDB_ORDERING:
			return cmp (LDB_ORDERING.index(aLdb),
				LDB_ORDERING.index(bLdb) )
		return -1

	elif bLdb in LDB_ORDERING:
		return 1

	return cmp(aLdb, bLdb)

def sortResultSet (results, ldbKeyCol, markerKeyCol):
	# sort 'results' by marker key and a custom logical database sort

	# compile a sortable list that is indexed into 'results'
	i = 0
	sortable = []		# each item is (marker key, ldb key, i)

	for row in results:
		sortable.append ( (row[markerKeyCol], row[ldbKeyCol], i) )
		i = i + 1

	# sort the new list
	sortable.sort (compareMarkerLdb)

	# reshuffle results into a new list based on newly-sorted 'sortable'
	newList = []
	for (markerKey, ldbKey, i) in sortable:
		newList.append (results[i])

	return newList

def excludeDuplicates (resultList, ldbKeyCol, markerKeyCol):
	# Purpose: remove from resultList any rows for which the same ldbKey
	#	and markerKey appear in multiple rows
	# Returns: updated version of 'resultList'
	# Assumes: nothing
	# Modifies: 'resultList'

	# We're going to do this in two passes.  First, we walk 'resultList'
	# and identify the (ldbKey, markerKey) pairs we need to remove.  Then
	# we will walk the list again and remove them.  This function, then,
	# will work regardless of the ordering of 'resultList'.

	seen = set()
	duplicated = set()

	i = len(resultList)
	while i > 0:
		i = i - 1
		pair = (resultList[i][ldbKeyCol], resultList[i][markerKeyCol])

		if pair in seen:
			duplicated.add(pair)
		else:
			seen.add(pair)

	# at this point, 'duplicated' contains the pairs we need to remove,
	# so we walk the list again and remove them

	i = len(resultList)
	while i > 0:
		i = i - 1
		pair = (resultList[i][ldbKeyCol], resultList[i][markerKeyCol])

		if pair in duplicated:
			del resultList[i]

	return resultList

def getDisplayText (ldbKey, ldbName, accID):
	# We can tweak the text of the link here, depending on the logical
	# database.  At present, we always use the logical db name.

	return ldbName

def getVersionForVistaPoint (version):
	# translate the coordinate version from our database to the version
	# identifier expected for the VISTA-Point link

	# currently no sophisticated logic; we just return mm10 (which is 
	# mouse build 38)

	return 'mm10'

def tweakHomologyValues (ldbKey, ldbName, url, associatedID):
	# allow tweaking of logical database-related values to allow overrides
	# and using some IDs to reach their non-standard sites.  (like using
	# Ensembl IDs to reach Gene Tree rather than Ensembl.)

	if ldbKey == ENSEMBL_GENE_MODEL:
		ldbName = GENE_TREE
		url = config.GENE_TREE_URL
		associatedID = None

	return ldbKey, ldbName, url, associatedID

###--- Classes ---###

class MarkerLinkGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the marker_link table
	# Has: queries to execute against the source database
	# Does: queries the source database for data we can use to pre-compute
	#	certain links, collates results, writes tab-delimited text
	#	file

	def getHumanHomologyRows (self):
		# returns a list with one row for each link for each human
		# marker which is included in a homology cluster.  Each row
		# includes:
		#	[ 'marker_key', 'link_group', 'sequence_num',
		#	'associated_id', 'display_text', 'url', 'has_markups',
		#	'use_new_window' ]
		# Assumes: input rows are ordered by logical db key

		links = []

		cols, rows = self.results[0]

		ldbKeyCol = Gatherer.columnNumber (cols, '_LogicalDB_key')
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		idCol = Gatherer.columnNumber (cols, 'accID')

		seqNum = 0

		logger.debug ('Found %d human homology links' % len(rows))

		rows = sortResultSet (rows, ldbKeyCol, markerKeyCol)
		logger.debug ('Sorted %d human homology links' % len(rows))

		for row in excludeDuplicates (rows, ldbKeyCol, markerKeyCol):
			ldbKey = row[ldbKeyCol]
			markerKey = row[markerKeyCol]
			accID = row[idCol]
			seqNum = seqNum + 1

			ldbName = Gatherer.resolve (ldbKey, 'acc_logicaldb',
				'_LogicalDB_key', 'name')

			# assumes each logical db has only one actual database
			# and thus one URL (safe assumption for our set of
			# logical databases here)

			ldbUrl = Gatherer.resolve (ldbKey, 'acc_actualdb',
				'_LogicalDB_key', 'url')

			fullUrl = ldbUrl.replace ('@@@@', accID)

			links.append ( [ markerKey, HOMOLOGY_LINK_GROUP,
				seqNum, accID,
				getDisplayText(ldbKey, ldbName, accID),
				fullUrl, NO_MARKUPS, NO_NEW_WINDOW ] )

		logger.debug ('Stored %d human homology links' % seqNum)
		return links

	def getMouseHomologyRows (self):
		# returns a list with one row for each link for each mouse
		# marker which is included in a homology cluster.  Each row
		# includes:
		#	[ 'marker_key', 'link_group', 'sequence_num',
		#	'associated_id', 'display_text', 'url', 'has_markups',
		#	'use_new_window' ]
		# Assumes: input rows are ordered by logical db key
		# Notes: We have links both by IDs and by coordinates.

		links = []

		# build the ID-based links first, as they need lower sequence
		# numbers for ordering

		cols, rows = self.results[1]

		ldbKeyCol = Gatherer.columnNumber (cols, '_LogicalDB_key')
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		idCol = Gatherer.columnNumber (cols, 'accID')

		seqNum = 0

		logger.debug ('Found %d ID-based mouse homology links' % \
			len(rows))
		rows = sortResultSet (rows, ldbKeyCol, markerKeyCol)
		logger.debug ('Sorted %d ID-based mouse homology links' % \
			len(rows))

		for row in excludeDuplicates (rows, ldbKeyCol, markerKeyCol):
			ldbKey = row[ldbKeyCol]
			markerKey = row[markerKeyCol]
			accID = row[idCol]
			seqNum = seqNum + 1

			ldbName = Gatherer.resolve (ldbKey, 'acc_logicaldb',
				'_LogicalDB_key', 'name')

			# assumes each logical db has only one actual database
			# and thus one URL (safe assumption for our set of
			# logical databases here)

			ldbUrl = Gatherer.resolve (ldbKey, 'acc_actualdb',
				'_LogicalDB_key', 'url')

			displayID = accID

			ldbKey, ldbName, ldbUrl, displayID = \
				tweakHomologyValues (ldbKey, ldbName, ldbUrl,
					displayID)

			fullUrl = ldbUrl.replace ('@@@@', accID)

			links.append ( [ markerKey, HOMOLOGY_LINK_GROUP,
				seqNum, displayID,
				getDisplayText(ldbKey, ldbName, accID),
				fullUrl, NO_MARKUPS, NO_NEW_WINDOW ] )

		# build the coordinate-based links next, as they should come
		# last in each marker's list of links

		# short-circuit if no VISTA-Point URL
		if not config.VISTA_POINT_URL:
			return links

		cols, rows = self.results[2]

		logger.debug ('Found %d coord-based mouse homology links' % \
			len(rows))

		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		chromCol = Gatherer.columnNumber (cols, 'genomicChromosome')
		startCol = Gatherer.columnNumber (cols, 'startCoordinate')
		endCol = Gatherer.columnNumber (cols, 'endCoordinate')
		strandCol = Gatherer.columnNumber (cols, 'strand')
		versionCol = Gatherer.columnNumber (cols, 'version')

		seen = set()	# track which marker keys we already handled

		for row in rows:
			markerKey = row[markerKeyCol]

			# don't make extra links if the marker happens to
			# return multiple coordinate rows -- just keep the
			# first one

			if markerKey in seen:
				continue
			seen.add(markerKey)

			chromosome = row[chromCol]
			startCoord = str(int(row[startCol]))
			endCoord = str(int(row[endCol]))
			version = getVersionForVistaPoint(row[versionCol])
			seqNum = seqNum + 1

			vistaPointUrl = config.VISTA_POINT_URL
			vistaPointUrl = vistaPointUrl.replace (
				'<version>', version)
			vistaPointUrl = vistaPointUrl.replace (
				'<chromosome>', chromosome)
			vistaPointUrl = vistaPointUrl.replace (
				'<startCoordinate>', startCoord)
			vistaPointUrl = vistaPointUrl.replace (
				'<endCoordinate>', endCoord)

			links.append ( [ markerKey, HOMOLOGY_LINK_GROUP,
				seqNum, None, VISTA_POINT, vistaPointUrl,
				NO_MARKUPS, NO_NEW_WINDOW ] ) 

		logger.debug ('Stored %d mouse homology links' % seqNum)
		return links

	def getOtherHomologyRows (self):
		# returns a list with one row for each link for each non-human,
		# non-mouse marker which is included in a homology cluster.
		# Each row includes:
		#	[ 'marker_key', 'link_group', 'sequence_num',
		#	'associated_id', 'display_text', 'url', 'has_markups',
		#	'use_new_window' ]
		# Assumes: input rows are ordered by logical db key

		links = []

		cols, rows = self.results[3]

		ldbKeyCol = Gatherer.columnNumber (cols, '_LogicalDB_key')
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		idCol = Gatherer.columnNumber (cols, 'accID')

		seqNum = 0

		logger.debug ('Found %d other homology links' % len(rows))

		rows = sortResultSet (rows, ldbKeyCol, markerKeyCol)
		logger.debug ('Sorted %d other homology links' % len(rows))

		for row in excludeDuplicates (rows, ldbKeyCol, markerKeyCol):
			ldbKey = row[ldbKeyCol]
			markerKey = row[markerKeyCol]
			accID = row[idCol]
			seqNum = seqNum + 1

			ldbName = Gatherer.resolve (ldbKey, 'acc_logicaldb',
				'_LogicalDB_key', 'name')

			# assumes each logical db has only one actual database
			# and thus one URL (safe assumption for our set of
			# logical databases here)

			ldbUrl = Gatherer.resolve (ldbKey, 'acc_actualdb',
				'_LogicalDB_key', 'url')

			fullUrl = ldbUrl.replace ('@@@@', accID)

			links.append ( [ markerKey, HOMOLOGY_LINK_GROUP,
				seqNum, accID,
				getDisplayText(ldbKey, ldbName, accID),
				fullUrl, NO_MARKUPS, NO_NEW_WINDOW ] )

		logger.debug ('Stored %d other homology links' % seqNum)
		return links

	def collateResults (self):
		self.finalColumns = [ 'marker_key', 'link_group',
			'sequence_num', 'associated_id', 'display_text',
			'url', 'has_markups', 'use_new_window' ]

		self.finalResults = self.getHumanHomologyRows() + \
			self.getMouseHomologyRows() + \
			self.getOtherHomologyRows()
		logger.debug ('Found %d total homology links' % \
			len(self.finalResults))
		return

###--- globals ---###

# present rules for links for markers (in order of display):
# 1. links for human genes:
#	a. HGNC (HGNC ID)
#	b. EntrezGene (EntrezGene ID)
#	c. OMIM (OMIM gene ID)
# 2. links for mouse genes:
#	a. MGI marker detail (MGI ID)
#	b. EntrezGene (EntrezGene ID)
#	c. GeneTree (Ensembl ID -- if only one Ensembl ID)
#	d. VISTA-Point (marker coordinates)
# 3. links for other organisms' genes:
#	a. EntrezGene (EntrezGene ID)

cmds = [
	# 0. human markers' IDs (for markers in a homology cluster)
	'''select a._LogicalDB_key,
		m._Marker_key,
		a.accID
	from mrk_marker m, acc_accession a
	where m._Organism_key = %d
		and m._Marker_key = a._Object_key
		and a._MGIType_key = %d
		and a._LogicalDB_key in (%d, %d, %d)
		and a.private = 0
		and a.preferred = 1
--		and exists (select 1 from MRK_Cluster mc,
--				MRK_ClusterMember mcm
--			where mcm._Marker_key = m._Marker_key
--			and mcm._Cluster_key = mc._Cluster_key
--			and mc._ClusterType_key = %d)''' % (
		HUMAN, MARKER, HGNC, ENTREZ_GENE, OMIM, HOMOLOGY_CLUSTER),

	# 1. mouse markers' IDs (for markers in a homology cluster)
	'''select a._LogicalDB_key,
		m._Marker_key,
		a.accID
	from mrk_marker m, acc_accession a
	where m._Organism_key = %d
		and m._Marker_key = a._Object_key
		and a._MGIType_key = %d
		and a._LogicalDB_key in (%d, %d, %d)
		and a.private = 0
		and a.preferred = 1
--		and exists (select 1 from MRK_Cluster mc,
--				MRK_ClusterMember mcm
--			where mcm._Marker_key = m._Marker_key
--			and mcm._Cluster_key = mc._Cluster_key
--			and mc._ClusterType_key = %d)''' % (
		MOUSE, MARKER, MGI, ENTREZ_GENE, ENSEMBL_GENE_MODEL,
		HOMOLOGY_CLUSTER),

	# 2. mouse markers' coordinates (for markers in a homology cluster)
	'''select mlc._Marker_key,
		mlc.sequenceNum,
		mlc.genomicChromosome,
		mlc.startCoordinate,
		mlc.endCoordinate,
		mlc.strand,
		mlc.version
	from mrk_location_cache mlc
	where mlc._Organism_key = %d
		and mlc.genomicChromosome is not null
		and mlc.startCoordinate is not null
		and mlc.endCoordinate is not null
--		and exists (select 1 from MRK_Cluster mc,
--				MRK_ClusterMember mcm
--			where mcm._Marker_key = mlc._Marker_key
--			and mcm._Cluster_key = mc._Cluster_key
--			and mc._ClusterType_key = %d)
	order by mlc._Marker_key, mlc.sequenceNum
	''' % (MOUSE, HOMOLOGY_CLUSTER),

	# 3. other species' markers (for markers in a homology cluster)
	'''select a._LogicalDB_key,
		m._Marker_key,
		a.accID
	from mrk_marker m, acc_accession a
	where m._Organism_key != %d
		and m._Organism_key != %d
		and m._Marker_key = a._Object_key
		and a._MGIType_key = %d
		and a._LogicalDB_key = %d
		and a.private = 0
		and a.preferred = 1
--		and exists (select 1 from MRK_Cluster mc,
--				MRK_ClusterMember mcm
--			where mcm._Marker_key = mlc._Marker_key
--			and mcm._Cluster_key = mc._Cluster_key
--			and mc._ClusterType_key = %d)''' % (
		HUMAN, MOUSE, MARKER, ENTREZ_GENE, HOMOLOGY_CLUSTER),
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, 'marker_key', 'link_group', 'sequence_num',
	'associated_id', 'display_text', 'url', 'has_markups',
	'use_new_window'
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_link'

# global instance of a MarkerLinkGatherer
gatherer = MarkerLinkGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
