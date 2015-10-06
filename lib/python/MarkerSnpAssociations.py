#!/usr/local/bin/python

# Module: MarkerSnpAssociations.py
# Purpose: to provide an easy means to determine which SNPs are associated
#	with which markers, to hide the complexity of in-sync versus
#	out-of-sync modes, to provide access to SNP counts per marker, etc.
# Notes: Historically this module cached data for all markers and SNPs, but
#	as of September 2013, this began blowing out the 4Gb memory limit.
#	Having tried several ways of optimizing memory usage with no success,
#	I opted to significantly revise this module to deal with only one
#	chromosome at a time. - jsb
# Important: As a result of the above Note, effective use of this module will
#	now require that we process markers for each chromosome together,
#	rather than just iterating over marker keys without considering the
#	markers' chromosomes.

import dbAgnostic
import logger
import config
import gc

###--- Globals ---###

# are the genome builds in-sync for marker and SNP coordinates?  assume that
# they are, unless we find otherwise
IN_SYNC = 1

try:
	if config.BUILDS_IN_SYNC == 0:
		IN_SYNC = 0
except:
	pass

# which chromosome's data are currently loaded?
CURRENT_CHROMOSOME = None

# maps from marker key to its chromosome (for CURRENT_CHROMOSOME)
MARKER_CHROMOSOME = {}

# maps from marker key to a dictionary keyed by its associated SNP keys
#	SNP_CACHE[markerKey] = { snpKey : 1 }
# now only contains markers for the CURRENT_CHROMOSOME
SNP_CACHE = {}

# maps from marker key to a dictionary keyed by its associated SNP keys where
# those SNPs map to multiple locations in the genome
#	MULTI_SNP_CACHE[markerKey] = { snpKey : 1 }
# now only contains markers for the CURRENT_CHROMOSOME
MULTI_SNP_CACHE = {}

# maps from SNP key to the ID for that SNP.
# now only contains markers for the CURRENT_CHROMOSOME
SNP_IDS = {}

# flag indicating whether accession IDs have been loaded or not
LOADED_IDS = False

###--- Private Functions ---###

def _setChromosome (chromosome):
	global CURRENT_CHROMOSOME, SNP_CACHE, MULTI_SNP_CACHE
	global LOADED_IDS, SNP_IDS

	# remember this chromosome and forget all the other data currently
	# loaded

	CURRENT_CHROMOSOME = chromosome

	# explicitly delete the caches and their contents to allow more
	# effective garbage collection

	for cache in [ SNP_CACHE, MULTI_SNP_CACHE, SNP_IDS ]:
		for key in cache.keys():
			del cache[key]

	del SNP_CACHE
	del MULTI_SNP_CACHE
	del SNP_IDS

	# reset the caches

	SNP_CACHE = {}
	MULTI_SNP_CACHE = {}
	SNP_IDS = {}
	LOADED_IDS = False

	if chromosome:
		logger.debug ("Switching to chromosome %s" % chromosome)

	gc.collect()
	logger.debug ("Ran garbage collector")
	return

def _getChromosome (markerKey):
	global MARKER_CHROMOSOME
	if markerKey in MARKER_CHROMOSOME:
		return CURRENT_CHROMOSOME

	# get all the markers that happen to be on the same chromosome with
	# the given markerKey
	# excluding QTL (marker_type 6) and heritable phenotypic marker (mcvterm_key 6238170)

	cmd = '''select mlc1._Marker_key, mlc1.chromosome
		from mrk_location_cache mlc1, mrk_location_cache mlc2,
			mrk_marker mm
		where mlc2._Marker_key = %d
			and mlc1._Marker_key = mm._Marker_key
                        and mm._Marker_Type_key != 6
			and not exists(select 1 from mrk_mcv_cache mcv where mcv._marker_key=mm._marker_key and mcv._mcvterm_key=6238170)
			and mm._Organism_key = 1
			and mm._Marker_Status_key in (1,3)
			and mlc1.startCoordinate is not null
			and mlc1.endCoordinate is not null
			and mlc2.chromosome = mlc1.chromosome''' % markerKey

	(cols, rows) = dbAgnostic.execute (cmd)

	if len(rows) == 0:
		return None

	mrkCol = dbAgnostic.columnNumber (cols, '_Marker_key')
	chrCol = dbAgnostic.columnNumber (cols, 'chromosome')

	MARKER_CHROMOSOME = {}		# reset the cache of markers

	for row in rows:
		MARKER_CHROMOSOME[row[mrkCol]] = 1

#	logger.debug('Found %d markers on chr %s' % (len(MARKER_CHROMOSOME),
#		row[chrCol]) )

	return row[chrCol]

def _addAssoc (snpCache, markerKey, snpKey, coord = None):
	global SNP_IDS

	# in order to count separate coordinates for a multi-coordinate SNP,
	# we need to count the SNP key / SNP coordinate pairs
	#pair = (snpKey, coord)

	# We now only want to count the distinct SNPs, not locations.
	pair = snpKey

	if snpCache.has_key(markerKey):
		snpCache[markerKey][pair] = 1
	else:
		snpCache[markerKey] = { pair : 1 }

	# initially, no ID found yet
	SNP_IDS[snpKey] = None
	return

def _loadDbSnpAssociations():
	# cache the SNP/marker associations made by dbSNP, exclude SNPs with
	# multiple locations

	global SNP_CACHE

	dbSnpQuery = '''select csm._Marker_key, csm._ConsensusSnp_key
		from snp_consensussnp_marker csm, mrk_location_cache mlc,
			snp_coord_cache scc
		where csm._Marker_key = mlc._Marker_key
			and csm._ConsensusSnp_key = scc._ConsensusSnp_key
			and scc.isMultiCoord = 0
			and mlc.chromosome = '%s' ''' % CURRENT_CHROMOSOME

	(cols, rows) = dbAgnostic.execute (dbSnpQuery)

	mrkCol = dbAgnostic.columnNumber (cols, '_Marker_key')
	snpCol = dbAgnostic.columnNumber (cols, '_ConsensusSnp_key')

	for row in rows:
		_addAssoc (SNP_CACHE, row[mrkCol], row[snpCol])

	logger.debug ('Cached %d dbSNP associations for chr %s' % (len(rows),
		CURRENT_CHROMOSOME) )
	return

def _loadDistanceAssociations():
	# cache additional SNP/marker associations for SNPs found within 2kb
	# of a marker's coordinates

	global SNP_CACHE

	# markers on a given chromosome, ordered by start coordinate
	# excluding QTL (marker_type 6) and heritable phenotypic marker (mcvterm_key 6238170)
	markerQuery = '''select distinct l._Marker_key, l.startCoordinate,
                        l.endCoordinate
                from mrk_location_cache l,mrk_marker m
                where l.chromosome = '%s'
                        and l.startCoordinate is not null
                        and l.endCoordinate is not null
                        and m._marker_key=l._marker_key
                        and m._Marker_Type_key!=6
			and not exists(select 1 from mrk_mcv_cache mcv where mcv._marker_key=m._marker_key and mcv._mcvterm_key=6238170)
			and m._Marker_Status_key in (1,3)
                order by l.startCoordinate, l.endCoordinate'''

	# SNPs on a given chromosome, ordered by start coordinate.  exclude
	# SNPs with multiple coordinates
	snpQuery = '''select distinct _ConsensusSnp_key, startCoordinate,
			isMultiCoord
		from snp_coord_cache
		where chromosome = '%s'
			and startCoordinate is not null
		order by startCoordinate'''

	# just do one chromosome now...

	chromosomes = [ CURRENT_CHROMOSOME ]

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
	return

def _loadSnpIDs():
	global SNP_IDS

	idQuery = '''select a._Object_key, a.accID
		from snp_accession a, snp_coord_cache c
		where a._MGIType_key = 30
			and a._Object_key = c._ConsensusSnp_key
			and c.chromosome = '%s' ''' % CURRENT_CHROMOSOME

	(cols, rows) = dbAgnostic.execute (idQuery)

	keyCol = dbAgnostic.columnNumber (cols, '_Object_key')
	idCol = dbAgnostic.columnNumber (cols, 'accID')

	logger.debug ('Found %d SNP IDs for chr %s' % (len(rows),
		CURRENT_CHROMOSOME) )

	for row in rows:
		snpKey = row[keyCol]

		# We only keep SNP IDs when they are associated with markers,
		# so first check to see if snpKey is in SNP_IDS.

		if SNP_IDS.has_key(snpKey):
			SNP_IDS[snpKey] = row[idCol]

	logger.debug ('Kept IDs for %d chr %s SNPs' % (len(SNP_IDS),
		CURRENT_CHROMOSOME) )
	return

def _initialize():
	global SNP_CACHE

	SNP_CACHE = {}

	_loadDbSnpAssociations()
	if IN_SYNC:
		_loadDistanceAssociations()

	logger.debug ("Finished init for MarkerSnpAssociations for chr %s" \
		% CURRENT_CHROMOSOME)
	return

###--- Functions ---###

def getSnps (markerKey,chromosome):
	# For the given marker key, return the keys for all the
	# single-coordinate SNPs that are associated with it

	global CURRENT_CHROMOSOME
	global MARKER_CHROMOSOME

	#chrom = _getChromosome(markerKey)
	#chrom=CURRENT_CHROMOSOME
	#if chromosome != CURRENT_CHROMOSOME:
		#chrom=_getChromosome(markerKey)
	#logger.debug("found chr=%s"%chrom)

	if chromosome != CURRENT_CHROMOSOME:
		CURRENT_CHROMOSOME = chromosome
		_setChromosome(chromosome)
		_initialize()

	#logger.debug("getSnpsInner() mkey=%s"%markerKey)
	if markerKey in SNP_CACHE:
		#logger.debug("getSnps() in cache mkey=%s"%markerKey)
		snpKeys = []
		#for (key, coord) in SNP_CACHE[markerKey].keys():
		for key in SNP_CACHE[markerKey].keys():
			snpKeys.append (key)
		return snpKeys
	return []

def getSnpCount (markerKey,chromosome):
	return len(getSnps(markerKey,chromosome))

def getMultiCoordSnps (markerKey,chromosome):
	# For the given marker key, return the keys for all the SNPs (either
	# single or multi-coordinate) that are associated with it.  Multi-
	# coordinate SNPs will appear multiple times in the list.

	global CURRENT_CHROMOSOME

	#chrom = _getChromosome(markerKey)

	if chromosome != CURRENT_CHROMOSOME:
		CURRENT_CHROMOSOME = chromosome
		_setChromosome(chromosome)
		_initialize()

	if MULTI_SNP_CACHE.has_key(markerKey):
		snpKeys = []
		#for (key, coord) in MULTI_SNP_CACHE[markerKey].keys():
		for key in MULTI_SNP_CACHE[markerKey].keys():
			snpKeys.append(key)
		return snpKeys
	return []

def getMultiCoordSnpCount (markerKey,chromosome):
	return len(getMultiCoordSnps(markerKey,chromosome))

def getSnpIDs (markerKey,chromosome):
	global LOADED_IDS
	#logger.debug("getSnps() mkey=%s"%markerKey)
	snpKeys = getSnps(markerKey,chromosome)
	#logger.debug("gotSnps() mkey=%s"%markerKey)

	if not LOADED_IDS:
		_loadSnpIDs()
		LOADED_IDS = True
		logger.debug ("Loaded SNP IDs for chr %s" % CURRENT_CHROMOSOME)

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

def getAllMarkerCounts():
	# get SNP counts for all markers which have SNP associations.
	# Returns: dictionary mapping from each marker key to a 2-item tuple
	#	with (SNP count, multi-location SNP count)

	markers = {}

	cmd = '''select distinct _Marker_key, chromosome
		from mrk_marker mm
		where _Organism_key = 1
			and _Marker_Type_key != 6
			and not exists(select 1 from mrk_mcv_cache mcv where mcv._marker_key=mm._marker_key and mcv._mcvterm_key=6238170)
			and _Marker_Status_key in (1,3)
			and _Organism_key = 1
		order by chromosome'''

	(cols, rows) = dbAgnostic.execute (cmd)

	mrkCol = dbAgnostic.columnNumber (cols, '_Marker_key')
	chrCol = dbAgnostic.columnNumber (cols, 'chromosome')

	for row in rows:
		markerKey = row[mrkCol]
		chromosome = row[chrCol]	

		snpCount = getSnpCount(markerKey,chromosome)
		multiCount = getMultiCoordSnpCount(markerKey,chromosome)

		if (snpCount > 0) or (multiCount > 0):
			markers[markerKey] = (snpCount, multiCount)

	return markers

def unload():
	# unload all the caches from memory

	_setChromosome(None)
	return

