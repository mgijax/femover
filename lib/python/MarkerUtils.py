# Module: MarkerUtils.py
# Purpose: to provide handy utility functions for dealing with mouse marker
#	data

import dbAgnostic
import logger
import gc
import GroupedList

###--- globals ---###

# marker key -> (genetic chrom, genomic chrom, start coord, end coord,
#	chromosome sequence number)
coordCache = {}	

# marker key -> symbol
symbolCache = {}

# marker key -> primary ID cache
idCache = {}

# cache of rows for traditional allele-to-marker relationships.  Each row is:
#	[ allele key, marker key, count type, count type sequence num ]
amRows = None

# cache of rows for 'mutation involves' allele-to-marker relationships, each:
#	[ allele key, marker key, count type, count type sequence num ]
miRows = None

# cache of rows for 'expresses component' allele-to-marker relationships, each:
#	[ allele key, marker key, count type, count type sequence num ]
ecRows = None

# constants specifying which set of marker/allele pairs to return
TRADITIONAL = 'traditional'
MUTATION_INVOLVES = 'mutation_involves'
EXPRESSES_COMPONENT = 'expresses_component'
UNIFIED = 'unified'

###--- private functions ---###

def _populateCoordCache():
	# populate the global 'coordCache' with location data for markers

	global coordCache

	cmd = '''select _Marker_key, genomicChromosome, chromosome,
			startCoordinate, endCoordinate, sequenceNum
		from mrk_location_cache
		where _Organism_key = 1'''

	(cols, rows) = dbAgnostic.execute(cmd)

	keyCol = dbAgnostic.columnNumber(cols, '_Marker_key')
	genomicChrCol = dbAgnostic.columnNumber(cols, 'genomicChromosome')
	geneticChrCol = dbAgnostic.columnNumber(cols, 'chromosome')
	startCol = dbAgnostic.columnNumber(cols, 'startCoordinate')
	endCol = dbAgnostic.columnNumber(cols, 'endCoordinate')
	seqNumCol = dbAgnostic.columnNumber(cols, 'sequenceNum')

	for row in rows:
		coordCache[row[keyCol]] = (row[geneticChrCol],
			row[genomicChrCol], row[startCol], row[endCol],
			row[seqNumCol])

	del cols
	del rows
	gc.collect()

	logger.debug ('Cached %d locations' % len(coordCache))
	return

def _populateSymbolCache():
	# populate the global 'symbolCache' with symbols for mouse markers

	global symbolCache

	cmd = '''select _Marker_key, symbol
		from mrk_marker
		where _Organism_key = 1
			and _Marker_Status_key in (1,3)'''

	(cols, rows) = dbAgnostic.execute(cmd)

	keyCol = dbAgnostic.columnNumber (cols, '_Marker_key')
	symbolCol = dbAgnostic.columnNumber (cols, 'symbol')

	for row in rows:
		symbolCache[row[keyCol]] = row[symbolCol]

	del cols
	del rows
	gc.collect()

	logger.debug ('Cached %d marker symbols' % len(symbolCache))
	return

def _populateIDCache():
	# populate the global 'idCache' with primary IDs for mouse markers

	global idCache

	cmd = '''select m._Marker_key, a.accID
		from mrk_marker m, acc_accession a
		where m._Organism_key = 1
			and m._Marker_Status_key in (1,3)
			and m._Marker_key = a._Object_key
			and a._MGIType_key = 2
			and a.private = 0
			and a.preferred = 1
			and a._LogicalDB_key = 1
			and a.prefixPart = 'MGI:' '''

	(cols, rows) = dbAgnostic.execute(cmd)

	keyCol = dbAgnostic.columnNumber (cols, '_Marker_key')
	idCol = dbAgnostic.columnNumber (cols, 'accID')

	for row in rows:
		idCache[row[keyCol]] = row[idCol]

	del cols
	del rows
	gc.collect()

	logger.debug ('Cached %d marker IDs' % len(idCache))
	return

def _populateMarkerAlleleCache():
	global amRows

	if amRows != None:
		return

	cmd1 = '''select distinct a._Marker_key,
			vt.term as countType,
			_Allele_key,
			vt.sequenceNum
		from all_allele a, voc_term vt
		where vt._Vocab_key = 38
			and vt.term not in ('Not Applicable', 'Not Specified',
				'Other')
			and vt._Term_key = a._Allele_Type_key
			and a.isWildType = 0
			and a._Marker_key is not null
			and exists (select 1 from mrk_marker m
				where a._Marker_key = m._Marker_key)
		order by a._Marker_key, vt.sequenceNum'''

	(cols1, rows1) = dbAgnostic.execute(cmd1)

	markerCol = dbAgnostic.columnNumber (cols1, '_Marker_key')
	alleleCol = dbAgnostic.columnNumber (cols1, '_Allele_key')
	typeCol = dbAgnostic.columnNumber (cols1, 'countType')
	seqNumCol = dbAgnostic.columnNumber (cols1, 'sequenceNum')

	amRows = []
	for row in rows1:
		amRows.append ( [ row[alleleCol], row[markerCol],
			row[typeCol], row[seqNumCol] ] )

	del cols1
	del rows1
	gc.collect()

	logger.debug ('Got %d traditional allele/marker pairs' % len(amRows))
	return 

def _getRelationships(typeName):
	# get a list of marker/allele pairs based on the given type of
	# relationships.

	cmd2 = '''select distinct r._Object_key_1 as allele_key,
			r._Object_key_2 as marker_key,
			t.term as countType,
			t.sequenceNum
		from mgi_relationship r,
			mgi_relationship_category c,
			all_allele a,
			voc_term t
		where c.name = '%s'
			and r._Category_key = c._Category_key
			and r._Object_key_1 = a._Allele_key
			and a._Allele_Type_key = t._Term_key''' % typeName

	(cols2, rows2) = dbAgnostic.execute(cmd2)

	markerCol = dbAgnostic.columnNumber (cols2, 'marker_key')
	alleleCol = dbAgnostic.columnNumber (cols2, 'allele_key')
	typeCol = dbAgnostic.columnNumber (cols2, 'countType')
	seqNumCol = dbAgnostic.columnNumber (cols2, 'sequenceNum')

	out = []
	for row in rows2:
		out.append ( [ row[alleleCol], row[markerCol], 
			row[typeCol], row[seqNumCol] ] ) 

	del cols2
	del rows2
	gc.collect()

	logger.debug ('Got %d %s allele/marker pairs' % (len(out), typeName))
	return out

def _populateMutationInvolvesCache():
	# populate the cache of marker/allele pairs based on
	# 'mutation_involves' relationships

	global miRows

	if miRows != None:
		return

	miRows = _getRelationships(MUTATION_INVOLVES)
	return

def _populateExpressesComponentCache():
	# populate the cache of marker/allele pairs based on
	# 'expresses_component' relationships

	global ecRows

	if ecRows != None:
		return

	ecRows = _getRelationships(EXPRESSES_COMPONENT)
	return

def _getMarkerAllelePairs(whichSet):
	# get a list of rows for allele/marker relationships, where each row is:
	#	[ allele key, marker key, count type, count type seq num ]
	# 'whichSet' should be one of TRADITIONAL, MUTATION_INVOVLES,
	# EXPRESSES_COMPONENT, or UNIFIED (which is the unique set of rows --
	# no duplicates)

	_populateMutationInvolvesCache()
	_populateExpressesComponentCache()
	_populateMarkerAlleleCache()

	if whichSet == TRADITIONAL:
		return amRows

	if whichSet == MUTATION_INVOLVES:
		return miRows

	if whichSet == EXPRESSES_COMPONENT:
		return ecRows

	unifiedList = []
	pairs = GroupedList.GroupedList()

	for myList in [ amRows, miRows, ecRows ]:
		for row in myList:
			pair = (row[0], row[1])		# allele + marker keys
			if not pairs.contains(pair):
				unifiedList.append(row)
				pairs.add(pair)

	logger.debug('Calculated set of %d distinct allele/marker pairs' % \
		len(unifiedList))

	del pairs
	gc.collect()

	return unifiedList

###--- functions dealing with location data ---###

def getMarkerCoords(markerKey):
	# get (genetic chrom, genomic chrom, start coord, end coord) for the
	# given marker key

	if len(coordCache) == 0:
		_populateCoordCache()

	if coordCache.has_key(markerKey):
		return coordCache[markerKey]

	return (None, None, None, None, 9999)

def getChromosome (marker):
	# get the chromosome for the given marker key, preferring
	# the genomic one over the genetic one

	(geneticChr, genomicChr, startCoord, endCoord, seqNum) = \
		getMarkerCoords(marker)

	if genomicChr:
		return genomicChr
	return geneticChr

def getChromosomeSeqNum (marker):
	# return the sequence number for sorting the chromosome of the given
	# marker key

	return getMarkerCoords(marker)[4]

def getStartCoord (marker):
	# return the start coordinate for the given marker key, or None if no
	# coordinates

	return getMarkerCoords(marker)[2]

def getEndCoord (marker):
	# return the end coordinate for the given marker key, or None if no
	# coordinates

	return getMarkerCoords(marker)[3] 

###--- functions dealing with accession IDs ---###

def getPrimaryID (markerKey):
	# return the primary MGI ID for the given mouse marker key, or None if
	# there is not one

	if len(idCache) == 0:
		_populateIDCache()

	if idCache.has_key(markerKey):
		return idCache[markerKey]
	return None

###--- functions dealing with nomenclature ---###

def getSymbol (markerKey):
	# return the symbol for the given marker key, or None if the key is
	# not for a mouse marker

	if len(symbolCache) == 0:
		_populateSymbolCache()

	if symbolCache.has_key(markerKey):
		return symbolCache[markerKey]
	return None

###--- functions dealing with allele counts ---###

def getAlleleCounts():
	# returns { marker key : count of all alleles }
	# includes both direct marker-to-allele relationships and ones from
	# 'mutation involves' and 'expresses component' relationships

	# each row has:
	# [ allele key, marker key, count type, count type order ]
	rows = _getMarkerAllelePairs(UNIFIED)

	alleles = {}		# alleles[markerKey] = [ allele keys ]

	for [ alleleKey, markerKey, countType, countTypeOrder ] in rows:
		if alleles.has_key(markerKey):
			if alleleKey not in alleles[markerKey]:
				alleles[markerKey].append(alleleKey)
		else:
			alleles[markerKey] = [ alleleKey ]

	counts = {}

	for markerKey in alleles.keys():
		counts[markerKey] = len(alleles[markerKey])

	logger.debug('Found allele counts for %d markers' % len(counts))
	return counts

def getAlleleCountsByType():
	# returns two-item tuple with:
	#	{ marker key : { count type : count of all alleles } }
	#	{ count type sequence num : count type }
	# includes both direct marker-to-allele relationships and ones from
	# 'mutation involves' and 'expresses component' relationships

	# each row has:
	# [ allele key, marker key, count type, count type order ]
	rows = _getMarkerAllelePairs(UNIFIED)

	m = {}		# m[markerKey] = { count type : [ allele keys ] }
	c = {}		# c[count type seq num] = count type

	for [ alleleKey, markerKey, countType, countTypeOrder ] in rows:

		# make sure we have the mapping from count type to its seq num
		if not c.has_key(countTypeOrder):
			c[countTypeOrder] = countType

		# track the alleles for each marker, separated by count type

		if not m.has_key(markerKey):
			m[markerKey] = { countType : [ alleleKey ] }

		elif not m[markerKey].has_key(countType):
			m[markerKey][countType] = [ alleleKey ]

		elif alleleKey not in m[markerKey][countType]:
			m[markerKey][countType].append (alleleKey)

	counts = {}

	for markerKey in m.keys():
		counts[markerKey] = {}

		for countType in m[markerKey].keys():
			counts[markerKey][countType] = \
				len(m[markerKey][countType])

	logger.debug('Found %d types of allele counts for %d markers' % (
		len(c), len(counts)) )
	return counts, c

def getMutationInvolvesCounts():
	# returns dictionary:
	#	{ marker key : count of alleles with that marker in a
	#		'mutation involves' relationship }

	rows = _getMarkerAllelePairs(MUTATION_INVOLVES)

	m = {}

	for [ alleleKey, markerKey, countType, countTypeOrder ] in rows:
		if not m.has_key(markerKey):
			m[markerKey] = [ alleleKey ]

		elif alleleKey not in m[markerKey]:
			m[markerKey].append (alleleKey)

	c = {}

	for markerKey in m.keys():
		c[markerKey] = len(m[markerKey])

	logger.debug('Found %d markers with mutation involves relationships' \
		% len(c))
	return c
