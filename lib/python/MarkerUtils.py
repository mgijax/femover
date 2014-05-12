# Module: MarkerUtils.py
# Purpose: to provide handy utility functions for dealing with mouse marker
#	data

import dbAgnostic
import logger

###--- globals ---###

# marker key -> (genetic chrom, genomic chrom, start coord, end coord,
#	chromosome sequence number)
coordCache = {}	

# marker key -> symbol
symbolCache = {}

# marker key -> primary ID cache
idCache = {}

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

	logger.debug ('Cached %d marker IDs' % len(idCache))
	return

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
