#!/usr/local/bin/python

# Module: MarkerSnpAssociations.py
# Purpose: to provide an easy means to determine which SNPs are associated
#	with which markers, to hide the complexity of in-sync versus
#	out-of-sync modes, to provide access to SNP counts per marker, etc.

import dbAgnostic
import logger
import config

###--- Globals ---###

# are the genome builds in-sync for marker and SNP coordinates?  assume that
# they are, unless we find otherwise
IN_SYNC = 1

try:
	if config.BUILDS_IN_SYNC == 0:
		IN_SYNC = 0
except:
	pass

# maps from marker key to a dictionary keyed by its associated SNP keys
#	SNP_CACHE[markerKey] = { snpKey : 1 }
SNP_CACHE = {}

# maps from marker key to a dictionary keyed by its associated SNP keys where
# those SNPs map to multiple locations in the genome
#	MULTI_SNP_CACHE[markerKey] = { snpKey : 1 }
MULTI_SNP_CACHE = {}

# maps from SNP key to its accession ID
SNP_IDS = {}
LOADED_IDS = False

###--- Private Functions ---###

def _addAssoc (snpCache, markerKey, snpKey, coord = None):
	global SNP_IDS

	# in order to count separate coordinates for a multi-coordinate SNP,
	# we need to count the SNP key / SNP coordinate pairs
	pair = (snpKey, coord)

	if snpCache.has_key(markerKey):
		snpCache[markerKey][pair] = 1
	else:
		snpCache[markerKey] = { pair : 1 }

	# initially, no ID found yet
	SNP_IDS[snpKey] = None
	return

def _loadDbSnpAssociations():
	# cache the SNP/marker associations made by dbSNP

	global SNP_CACHE

	dbSnpQuery = '''select _Marker_key, _ConsensusSnp_key
		from snp_consensussnp_marker'''

	(cols, rows) = dbAgnostic.execute (dbSnpQuery)

	mrkCol = dbAgnostic.columnNumber (cols, '_Marker_key')
	snpCol = dbAgnostic.columnNumber (cols, '_ConsensusSnp_key')

	for row in rows:
		_addAssoc (SNP_CACHE, row[mrkCol], row[snpCol])

	logger.debug ('Cached %d dbSNP associations' % len(rows))
	return

def _loadDistanceAssociations():
	# cache additional SNP/marker associations for SNPs found within 2kb
	# of a marker's coordinates

	global SNP_CACHE

	# markers on a given chromosome, ordered by start coordinate
	markerQuery = '''select distinct l._Marker_key, l.startCoordinate,
                        l.endCoordinate
                from mrk_location_cache l,mrk_marker m
                where l.chromosome = '%s'
                        and l.startCoordinate is not null
                        and l.endCoordinate is not null
                        and m._marker_key=l._marker_key
                        and m._Marker_Type_key!=6
                order by l.startCoordinate, l.endCoordinate'''

	# SNPs on a given chromosome, ordered by start coordinate.  exclude
	# SNPs with multiple coordinates
	snpQuery = '''select distinct _ConsensusSnp_key, startCoordinate,
			isMultiCoord
		from snp_coord_cache
		where chromosome = '%s'
			and startCoordinate is not null
		order by startCoordinate'''

	# ordered list of mouse chromosomes
	chromosomeQuery = '''select distinct chromosome
		from mrk_chromosome
		where _Organism_key = 1'''

	# we process this chromosome by chromosome in code because the SQL for
	# the computation was too slow

	# get the ordered list of chromosomes

	(cols, rows) = dbAgnostic.execute (chromosomeQuery)

	chromosomes = []
	for row in rows:
		chromosomes.append (row[0])

	chromosomes.sort()

	# walk through chromosomes

	for chromosome in chromosomes:
		# get the ordered list of markers for this chromosome

		(cols, rows) = dbAgnostic.execute (markerQuery % chromosome)

		mrkCol = dbAgnostic.columnNumber (cols, '_Marker_key')
		startCol = dbAgnostic.columnNumber (cols, 'startCoordinate')
		endCol = dbAgnostic.columnNumber (cols, 'endCoordinate')

		# need to track the first marker on the chromosome to overlap
		# each Mb.  This will allow us to quickly find a rough start
		# marker when looking for overlaps for any given SNP.  (The
		# cached marker may not overlap the SNP, but it will be
		# relatively close and there will be no other overlapping
		# markers before it.)
		# Update: revised to be more granular, according to 'factor'
		byMb = {}		# mb -> index into 'markers'

		factor = 100000		# size of chunks for 'byMb'

		markers = []

		for row in rows:
			# adjust the marker's coordinates by 2kb

			markers.append ( (row[mrkCol], row[startCol] - 2000,
				row[endCol] + 2000) )

			# flag each Mb which this marker overlaps, if not
			# already flagged by an earlier marker

			startMb = int(row[startCol] - 2000) / factor
			endMb = int(row[endCol] + 2000) / factor

			for mb in range(startMb, endMb + 1):
				if not byMb.has_key(mb):
					byMb[mb] = len(markers) - 1

		markerCount = len(markers) 

		logger.debug ('Found %d markers on chr %s' % (markerCount,
			chromosome))

		# walk the SNPs from the same chromosome, finding which
		# markers they overlap

		# position in 'markers' of the marker we are considering
		m = 0	

		(cols, rows) = dbAgnostic.execute (snpQuery % chromosome)

		snpCol = dbAgnostic.columnNumber (cols, '_ConsensusSnp_key')
		startCol = dbAgnostic.columnNumber (cols, 'startCoordinate')
		multiCol = dbAgnostic.columnNumber (cols, 'isMultiCoord')

		added = 0

		logger.debug ('Found %d SNPs on chr %s' % (len(rows),
			chromosome))

		# Of note, for SNPs with multiple locations, if a marker is
		# associated with any of the locations for the SNP, then it
		# should be associated with all locations for the SNP.  So,
		# we first compile a dictionary with locations for each
		# multi-location SNP.  Currently no SNPs have locations on
		# different chromosomes.

		multiLoc = {}	# snp key -> [ coord 1, coord 2, ... ]

		for row in rows:
			if row[multiCol] == 0:
				continue

			snpStart = row[startCol]
			snpKey = row[snpCol]

			if multiLoc.has_key(snpKey):
				multiLoc[snpKey].append (snpStart)
			else:
				multiLoc[snpKey] = [ snpStart ]

		logger.debug ('%d SNPs have 2+ locations' % len(multiLoc))

		# now go through and associate SNPs and markers

		for row in rows:
			snpStart = row[startCol]

			if row[multiCol] == 0:
				cache = SNP_CACHE
			else:
				cache = MULTI_SNP_CACHE

			# lookup the first marker which overlaps the Mb in
			# which this SNP occurs

			mb = int(snpStart) / factor
			if not byMb.has_key(mb):
				# no markers overlapping this Mb, go to the
				# next SNP
				continue

			m = byMb[mb]
			(mrkKey, mrkStart, mrkEnd) = markers[m]

			# now traverse markers to the right until we find the
			# first one that starts to the right of the SNP.  At
			# that point, we're done with this SNP.

			while (m < markerCount) and (mrkStart <= snpStart):

				# if the SNP start coordinate falls between
				# the marker's two coordinates, note the
				# overlap

				if mrkStart <= snpStart <= mrkEnd:
				    if row[multiCol] == 0:
					_addAssoc (cache, mrkKey, row[snpCol])
				    else:
					# We need to add in all locations for
					# this SNP, not just the location we
					# found.

					for loc in multiLoc[row[snpCol]]:
					    _addAssoc (cache, mrkKey,
						row[snpCol], loc)
				    added = added + 1

				m = m + 1

				if m < markerCount:
					(mrkKey,mrkStart,mrkEnd) = markers[m]

		logger.debug ('Found %d distance assoc for chr %s' % (added,
			chromosome))

	logger.debug ('Cached distance-based associations')
	return

def _loadSnpIDs():
	global SNP_IDS

	idQuery = '''select _Object_key, accID
		from snp_accession
		where _MGIType_key = 30'''

	(cols, rows) = dbAgnostic.execute (idQuery)

	keyCol = dbAgnostic.columnNumber (cols, '_Object_key')
	idCol = dbAgnostic.columnNumber (cols, 'accID')

	logger.debug ('Found %d SNP IDs' % len(rows))

	for row in rows:
		snpKey = row[keyCol]

		if SNP_IDS.has_key(snpKey):
			SNP_IDS[snpKey] = row[idCol]

	logger.debug ('Kept IDs for %d SNPs' % len(SNP_IDS))
	return

def _initialize():
	global SNP_CACHE

	SNP_CACHE = {}

	_loadDbSnpAssociations()
	if IN_SYNC:
		_loadDistanceAssociations()
	return

###--- Functions ---###

def getSnps (markerKey):
	# For the given marker key, return the keys for all the
	# single-coordinate SNPs that are associated with it

	if not SNP_CACHE:
		_initialize()

	if SNP_CACHE.has_key(markerKey):
		snpKeys = []
		for (key, coord) in SNP_CACHE[markerKey].keys():
			snpKeys.append (key)
		return snpKeys
	return []

def getSnpCount (markerKey):
	return len(getSnps(markerKey))

def getMultiCoordSnps (markerKey):
	# For the given marker key, return the keys for all the SNPs (either
	# single or multi-coordinate) that are associated with it.  Multi-
	# coordinate SNPs will appear multiple times in the list.

	if not SNP_CACHE:
		_initialize()

	if MULTI_SNP_CACHE.has_key(markerKey):
		snpKeys = []
		for (key, coord) in MULTI_SNP_CACHE[markerKey].keys():
			snpKeys.append(key)
		return snpKeys
	return []

def getMultiCoordSnpCount (markerKey):
	return len(getMultiCoordSnps(markerKey))

def getSnpIDs (markerKey):
	global LOADED_IDS
	snpKeys = getSnps(markerKey)

	if not LOADED_IDS:
		_loadSnpIDs()
		LOADED_IDS = True

	snpIds = []
	for key in snpKeys:
		if SNP_IDS.has_key(key):
			snpIds.append (SNP_IDS[key])
		else:
			snpIds.append ('key:%s' % str(key))
	snpIds.sort()
	return snpIds

def isInSync():
	return IN_SYNC

def getMarkerKeys():
	# get all marker keys which have SNPs
	if not SNP_CACHE:
		_initialize()

	markerKeys = {}

	for cache in [ SNP_CACHE, MULTI_SNP_CACHE ]:
		for key in cache.keys():
			markerKeys[key] = 1

	keys = markerKeys.keys()
	keys.sort()

	return keys
