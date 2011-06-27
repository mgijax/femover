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

# maps from SNP key to its accession ID
SNP_IDS = {}
LOADED_IDS = False

###--- Private Functions ---###

def _addAssoc (markerKey, snpKey):
	global SNP_CACHE, SNP_IDS

	if SNP_CACHE.has_key(markerKey):
		SNP_CACHE[markerKey][snpKey] = 1
	else:
		SNP_CACHE[markerKey] = { snpKey : 1 }

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
		_addAssoc (row[mrkCol], row[snpCol])

	logger.debug ('Cached %d dbSNP associations' % len(rows))
	return

def _loadDistanceAssociations():
	# cache additional SNP/marker associations for SNPs found within 2kb
	# of a marker's coordinates

	global SNP_CACHE

	# markers on a given chromosome, ordered by start coordinate
	markerQuery = '''select distinct _Marker_key, startCoordinate,
			endCoordinate
		from mrk_location_cache
		where chromosome = '%s'
			and startCoordinate is not null
			and endCoordinate is not null
		order by startCoordinate, endCoordinate'''

	# SNPs on a given chromosome, ordered by start coordinate
	snpQuery = '''select distinct _ConsensusSnp_key, startCoordinate
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

		markers = []

		for row in rows:
			# adjust the marker's coordinates by 2kb

			markers.append ( (row[mrkCol], row[startCol] - 2000,
				row[endCol] + 2000) )

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

		added = 0

		logger.debug ('Found %d SNPs on chr %s' % (len(rows),
			chromosome))

		for row in rows:
			snpStart = row[startCol]

			# SNPs are ordered, so markers matching the current
			# SNP should be near the prior marker we looked at

			# go back to find a marker that precedes the SNP, as
			# we will start there

			(mrkKey, mrkStart, mrkEnd) = markers[m]

			while (m > 0) and (mrkEnd > snpStart):
				m = m - 1
				(mrkKey, mrkStart, mrkEnd) = markers[m]

			# now traverse markers to the right until we find the
			# first one that starts to the right of the SNP

			while (m < markerCount) and (mrkStart <= snpStart):

				# if the SNP start coordinate falls between
				# the marker's two coordinates, note the
				# overlap

				if mrkStart <= snpStart <= mrkEnd:
					_addAssoc (mrkKey, row[snpCol])
					added = added + 1

				m = m + 1

				if m < markerCount:
					(mrkKey,mrkStart,mrkEnd) = markers[m]

			if m >= markerCount:
				m = markerCount - 1

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
	if not SNP_CACHE:
		_initialize()

	if SNP_CACHE.has_key(markerKey):
		return SNP_CACHE[markerKey].keys()
	return []

def getSnpCount (markerKey):
	return len(getSnps(markerKey))

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
			snpIds.append ('key:%d' % key)
	snpIds.sort()
	return snpIds

def isInSync():
	return IN_SYNC
