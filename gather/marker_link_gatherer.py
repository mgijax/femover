#!/usr/local/bin/python
# 
# gathers data for the 'marker_link' table in the front-end database

import config
import Gatherer
import logger
import os
import MarkerUtils

###--- Constants ---###

# logical db
MGI = 1				
OMIM = 15
ENTREZ_GENE = 55
HGNC = 64
ENSEMBL_GENE_MODEL = 60

# preferred ordering for human/mouse links, by logical database
LDB_ORDERING = [ HGNC, MGI, ENTREZ_GENE, OMIM, ENSEMBL_GENE_MODEL ]

# organism 
MOUSE = 1			
HUMAN = 2
ZEBRAFISH = 84
CHICKEN = 63
XENBASE = 95

# MGI Type
MARKER = 2			

# marker cluster type (vocab term)
HOMOLOGY_CLUSTER = 9272150	

# marker cluster source (vocab term)
ZFIN_SOURCE = 13575996
GEISHA_SOURCE = 13575998
XENBASE_SOURCE = 13611349

# link group name
HOMOLOGY_LINK_GROUP = 'homology gene links'	
ZFIN_EXPRESSION_LINK_GROUP = 'zfin expression links'
GEISHA_EXPRESSION_LINK_GROUP = 'geisha expression links'
XENBASE_EXPRESSION_LINK_GROUP = 'xenbase expression links'

# markup?
NO_MARKUPS = 0
HAS_MARKUPS = 1

# open in a new window?
NO_NEW_WINDOW = 0
USE_NEW_WINDOW = 1

VISTA_POINT = 'VISTA-Point'
GENE_TREE = 'Gene Tree'

# URL to expression data at zfin; substitute in an NCBI ID for a Zfin marker
zfin_url = '''http://zfin.org/cgi-bin/webdriver?MIval=aa-xpatselect.apg&query_results=exist&START=0&searchtype=equals&gene_name=%s'''

# URL to expression data at geisha; substitute in an NCBI ID for a chicken
# marker
geisha_url = 'http://geisha.arizona.edu/geisha/search.jsp?search=NCBI+ID&text=%s'

# URL to express data at xenbase; substiture in an ID
xenbase_url = 'http://xenbase.org/gene/expression.do?method=displayGenePageExpression&entrezId=%s&tabId=1'

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
	for (markerKey, ldbKey, i) in sortable:		newList.append (results[i])

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
			vistaPointUrl = vistaPointUrl.replace ( '<version>', version)
			vistaPointUrl = vistaPointUrl.replace ( '<chromosome>', chromosome)
			vistaPointUrl = vistaPointUrl.replace ( '<startCoordinate>', startCoord)
			vistaPointUrl = vistaPointUrl.replace ( '<endCoordinate>', endCoord)

			links.append ( [ markerKey, HOMOLOGY_LINK_GROUP, seqNum, None, VISTA_POINT, vistaPointUrl, NO_MARKUPS, NO_NEW_WINDOW ] ) 

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

			links.append ( [ markerKey, HOMOLOGY_LINK_GROUP, seqNum, accID, getDisplayText(ldbKey, ldbName, accID), fullUrl, NO_MARKUPS, NO_NEW_WINDOW ] )

		logger.debug ('Stored %d other homology links' % seqNum)
		return links

	def getOrganismRows(self, resultIndex, markerKeyColumn, organismSymbolColumn, organismIdColumn, organismUrl, linkGroup):
		cols, rows = self.results[resultIndex]

		markerKeyCol = Gatherer.columnNumber (cols, markerKeyColumn)
		symbolCol = Gatherer.columnNumber (cols, organismSymbolColumn)
		idCol = Gatherer.columnNumber (cols, organismIdColumn)
		
		seqNum = 0
		hasMarkups = 0
		useNewWindow = 1

		out = []

		for row in rows:
			markerKey = row[markerKeyCol]
			organismSymbol = row[symbolCol]
			organismID = row[idCol]

			seqNum = seqNum + 1

			out.append( [ markerKey, linkGroup, seqNum, organismID, organismSymbol, organismUrl % organismID, hasMarkups, useNewWindow ] )

		logger.debug('Returning %d rows for expression links' % len(out))
		return out

	def collateResults (self):
		self.finalColumns = [ 'marker_key', 'link_group',
			'sequence_num', 'associated_id', 'display_text',
			'url', 'has_markups', 'use_new_window' ]

		self.finalResults = self.getHumanHomologyRows() + \
				    self.getMouseHomologyRows() + \
				    self.getOtherHomologyRows() + \
				    self.getOrganismRows(4, 'mouse_marker_key', 'zfin_symbol', 'zfin_entrezgene_id', zfin_url, ZFIN_EXPRESSION_LINK_GROUP) + \
				    self.getOrganismRows(5, 'mouse_marker_key', 'geisha_symbol', 'geisha_entrezgene_id', geisha_url, GEISHA_EXPRESSION_LINK_GROUP) + \
				    self.getOrganismRows(6, 'mouse_marker_key', 'xenbase_symbol', 'xenbase_entrezgene_id', xenbase_url, XENBASE_EXPRESSION_LINK_GROUP)

		logger.debug ('Found %d total links' % \
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
	# 0. human markers' IDs
	'''select a._LogicalDB_key,
		m._Marker_key,
		a.accID
	from mrk_marker m, acc_accession a
	where m._Organism_key = %d
		and m._Marker_key = a._Object_key
		and a._MGIType_key = %d
		and a._LogicalDB_key in (%d, %d, %d)
		and a.private = 0
		and a.preferred = 1''' % (
			HUMAN, MARKER, HGNC, ENTREZ_GENE, OMIM),

	# 1. mouse markers' IDs
	'''select a._LogicalDB_key,
		m._Marker_key,
		a.accID
	from mrk_marker m, acc_accession a
	where m._Organism_key = %d
		and m._Marker_key = a._Object_key
		and a._MGIType_key = %d
		and a._LogicalDB_key in (%d, %d, %d)
		and a.private = 0
		and a.preferred = 1''' % (
			MOUSE, MARKER, MGI, ENTREZ_GENE, ENSEMBL_GENE_MODEL),

	# 2. mouse markers' coordinates
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
		and mlc.endCoordinate is not null''' % MOUSE, 

	# 3. other species' markers
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
		and a.preferred = 1''' % (
			HUMAN, MOUSE, MARKER, ENTREZ_GENE),

	# 4. data for ZFIN expression links (via homology)
	'''select distinct m._Marker_key as mouse_marker_key,
		zm.symbol as zfin_symbol,
		za.accID as zfin_entrezgene_id
	from mrk_marker m, mrk_clustermember mcm, mrk_cluster mc,
		mrk_clustermember zcm, mrk_marker zm, acc_accession za
	where m._Marker_Status_key in (1,3)
		and m._Organism_key = %d
		and m._Marker_key = mcm._Marker_key
		and mcm._Cluster_key = mc._Cluster_key
		and mc._ClusterType_key = %d
		and mc._ClusterSource_key = %d
		and mc._Cluster_key = zcm._Cluster_key
		and zcm._Marker_key = zm._Marker_key
		and zm._Organism_key = %d
		and zcm._Marker_key = za._Object_key
		and za._MGIType_key = %d
		and za._LogicalDB_key = %d
		and za.private = 0
		and za.preferred = 1
	order by m._Marker_key, zm.symbol''' % (MOUSE, HOMOLOGY_CLUSTER,
			ZFIN_SOURCE, ZEBRAFISH, MARKER, ENTREZ_GENE),

	# 5. data for chicken expression links (via homology)
	'''select distinct m._Marker_key as mouse_marker_key,
		zm.symbol as geisha_symbol,
		za.accID as geisha_entrezgene_id
	from mrk_marker m, mrk_clustermember mcm, mrk_cluster mc,
		mrk_clustermember zcm, mrk_marker zm, acc_accession za
	where m._Marker_Status_key in (1,3)
		and m._Organism_key = %d
		and m._Marker_key = mcm._Marker_key
		and mcm._Cluster_key = mc._Cluster_key
		and mc._ClusterType_key = %d
		and mc._ClusterSource_key = %d
		and mc._Cluster_key = zcm._Cluster_key
		and zcm._Marker_key = zm._Marker_key
		and zm._Organism_key = %d
		and zcm._Marker_key = za._Object_key
		and za._MGIType_key = %d
		and za._LogicalDB_key = %d
		and za.private = 0
		and za.preferred = 1
	order by m._Marker_key, zm.symbol''' % (MOUSE, HOMOLOGY_CLUSTER,
			GEISHA_SOURCE, CHICKEN, MARKER, ENTREZ_GENE),
	
	# 6. data for xenbase expression links (via homology)
	'''select distinct m._Marker_key as mouse_marker_key,
		zm.symbol as xenbase_symbol,
		za.accID as xenbase_entrezgene_id
	from mrk_marker m, mrk_clustermember mcm, mrk_cluster mc,
		mrk_clustermember zcm, mrk_marker zm, acc_accession za
	where m._Marker_Status_key in (1,3)
		and m._Organism_key = %d
		and m._Marker_key = mcm._Marker_key
		and mcm._Cluster_key = mc._Cluster_key
		and mc._ClusterType_key = %d
		and mc._ClusterSource_key = %d
		and mc._Cluster_key = zcm._Cluster_key
		and zcm._Marker_key = zm._Marker_key
		and zm._Organism_key = %d
		and zcm._Marker_key = za._Object_key
		and za._MGIType_key = %d
		and za._LogicalDB_key = %d
		and za.private = 0
		and za.preferred = 1
	order by m._Marker_key, zm.symbol''' % (MOUSE, HOMOLOGY_CLUSTER,
			XENBASE_SOURCE, XENBASE, MARKER, ENTREZ_GENE),
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
