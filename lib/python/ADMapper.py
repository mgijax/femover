# Module: ADMapper.py
# Purpose: to map from an anatomical dictionary (AD) term to its corresponding
#	EMAPS term
# Author: jsb

import dbAgnostic
import logger
import types
import gc

###--- Constants ---###

EMAPS_VOCAB = 91
TERM_MGITYPE = 13
STRUCTURE_MGITYPE = 38

###--- Globals ---###

INITIALIZED = False

# maps from an AD-related ID to its structure key
AD_ID_TO_KEY = {}

# maps from an EMAPS key to the primary ID for that term
EMAPS_KEY_TO_ID = {}

# maps from an EMAPS ID to the key for its term
EMAPS_ID_TO_KEY = {}

# maps from an AD structure key to its equivalent EMAPS term key
AD_KEY_TO_EMAPS_KEY = {}

# maps from other (non-AD) IDs to a particular EMAPS term key
OTHER_ID_TO_EMAPS_KEY = {}

# maps from EMAPS term key to the text of its term
EMAPS_KEY_TO_TERM = {}

# number of sets filtered so far
FILTERED_SET = 0

###--- Private Functions ---###

def _loadAnatomicalDictionaryIDs():
	# load IDs from the database and map them to AD structure keys

	global AD_ID_TO_KEY

	AD_ID_TO_KEY = {}

	# IDs associated through ACC_Accession

	cmd1 = '''select _Object_key as _Structure_key, accID
		from acc_accession
		where _MGIType_key = %d''' % STRUCTURE_MGITYPE

	(cols, rows) = dbAgnostic.execute(cmd1)

	keyCol = dbAgnostic.columnNumber (cols, '_Structure_key')
	idCol = dbAgnostic.columnNumber (cols, 'accID')

	for row in rows:
		accID = row[idCol].upper()
		key = row[keyCol]

		if not accID:
			continue

		if AD_ID_TO_KEY.has_key(accID):
			logger.debug(
				'Duplicate ID: %s (replaced key %d with %d)' \
				% (row[idCol], AD_ID_TO_KEY[accID], key) )
		AD_ID_TO_KEY[accID] = key

	logger.debug ('Retrieved %d IDs from ACC_Accession' % len(AD_ID_TO_KEY))

	# Old Edinburgh IDs (numeric component of old EMAP IDs)

	cmd2 = '''select _Structure_key, edinburghKey as accID
		from gxd_structure'''

	(cols, rows) = dbAgnostic.execute(cmd2)
	
	keyCol = dbAgnostic.columnNumber (cols, '_Structure_key')
	idCol = dbAgnostic.columnNumber (cols, 'accID')

	for row in rows:
		accID = str(row[idCol])
		key = row[keyCol]

		if (not accID) or (accID == 'None'):
			continue

		if AD_ID_TO_KEY.has_key(accID):
			logger.debug(
				'Duplicate ID: %s (replaced key %d with %d)' \
				% (row[idCol], AD_ID_TO_KEY[accID], key) )
		AD_ID_TO_KEY[accID] = key

	logger.debug ('Retrieved %d old EMAP IDs from GXD_Structure' % \
		len(rows))
	return

def _loadEmapsIDs():
	# load EMAPS IDs and their respective term keys from the database

	global EMAPS_ID_TO_KEY, EMAPS_KEY_TO_ID

	EMAPS_ID_TO_KEY = {}
	EMAPS_KEY_TO_ID = {}

	cmd = '''select a._Object_key as _Term_key,
			a.accID,
			a.preferred
		from acc_accession a,
			voc_term t
		where a._MGIType_key = %d
			and a._Object_key = t._Term_key
			and t._Vocab_key = %d''' % (TERM_MGITYPE, EMAPS_VOCAB)

	(cols, rows) = dbAgnostic.execute(cmd)

	keyCol = dbAgnostic.columnNumber (cols, '_Term_key')
	idCol = dbAgnostic.columnNumber (cols, 'accID')
	preferredCol = dbAgnostic.columnNumber (cols, 'preferred')

	for row in rows:
		accID = row[idCol].upper()
		key = row[keyCol]
		preferred = row[preferredCol]

		if not accID:
			continue

		# map from EMAPS ID to a single term key for each
		# (includes secondary IDs)

		if EMAPS_ID_TO_KEY.has_key(accID):
			logger.debug(
				'Duplicate ID: %s (replaced key %d with %d)' \
				% (row[idCol], EMAPS_ID_TO_KEY[accID], key) )

		EMAPS_ID_TO_KEY[accID] = key

		# map from an EMAPS term key to its preferred ID
		# (ignores secondary IDs)

		if not preferred:
			continue

		if EMAPS_KEY_TO_ID.has_key(key):
			logger.debug(
				'Key (%d) has two primary IDs: %s and %s' \
				% (key, EMAPS_KEY_TO_ID[key], row[idCol]) )

		EMAPS_KEY_TO_ID[key] = row[idCol]

	logger.debug('Loaded %d EMAPS ID/key mappings' % len(EMAPS_KEY_TO_ID))
	return

def _loadMapping():
	# load the mapping from AD to EMAPS
	# Assumes: 1. _loadEmapsIDs() has already been called
	#	2. _loadAnatomicalDictionaryIDs() has already been called

	global AD_KEY_TO_EMAPS_KEY, OTHER_ID_TO_EMAPS_KEY

	AD_KEY_TO_EMAPS_KEY = {}
	OTHER_ID_TO_EMAPS_KEY = {}

	cmd = '''select accID, emapsID
		from mgi_emaps_mapping'''

	(cols, rows) = dbAgnostic.execute(cmd)

	idCol = dbAgnostic.columnNumber (cols, 'accID')
	emapsCol = dbAgnostic.columnNumber (cols, 'emapsID')

	for row in rows:
		oldID = row[idCol].upper()
		emapsID = row[emapsCol].upper()

		structureKey = None
		termKey = None

		if AD_ID_TO_KEY.has_key(oldID):
			structureKey = AD_ID_TO_KEY[oldID]
		else:
			logger.debug (
				'Found ID unrelated to AD structure: %s' \
				% row[idCol])

		if EMAPS_ID_TO_KEY.has_key(emapsID):
			termKey = EMAPS_ID_TO_KEY[emapsID]
		else:
			logger.debug ('Found ID unrelated to EMAPS term: %s' \
				% row[emapsCol])

		if termKey and structureKey:
			AD_KEY_TO_EMAPS_KEY[structureKey] = termKey

		elif termKey:
			OTHER_ID_TO_EMAPS_KEY[oldID] = termKey

		else:
			logger.debug (
				'Bad mapping (no structure, no term): %s to %s'\
				% (row[idCol], row[emapsCol]) )

	logger.debug ('Mapped %d AD structures to EMAPS terms' % \
		len(AD_KEY_TO_EMAPS_KEY))
	logger.debug ('Mapped %d other IDs to EMAPS terms' % \
		len(OTHER_ID_TO_EMAPS_KEY))
	return

def _loadEmapsTerms():
	# load the text of the terms for EMAPS

	global EMAPS_KEY_TO_TERM

	EMAPS_KEY_TO_TERM = {}

	cmd = '''select t._Term_key, t.term
		from voc_term t
		where t._Vocab_key = %d''' % EMAPS_VOCAB

	(cols, rows) = dbAgnostic.execute(cmd)

	keyCol = dbAgnostic.columnNumber (cols, '_Term_key')
	termCol = dbAgnostic.columnNumber (cols, 'term')

	for row in rows:
		EMAPS_KEY_TO_TERM[row[keyCol]] = row[termCol]

	logger.debug('Cached text of %d EMAPS terms' % len(EMAPS_KEY_TO_TERM)) 
	return

def _initialize():
	global INITIALIZED

	if INITIALIZED:
		return

	_loadAnatomicalDictionaryIDs()
	_loadEmapsIDs()
	_loadMapping() 
	_loadEmapsTerms()

	INITIALIZED = True
	return

###--- Public Functions ---###

def getEmapsKey (structureKey):
	_initialize()

	if AD_KEY_TO_EMAPS_KEY.has_key(structureKey):
		return AD_KEY_TO_EMAPS_KEY[structureKey]
	return None

def getEmapsKeyByID (accID):
	_initialize()

	if AD_ID_TO_KEY.has_key(accID):
		return getEmapsKey(AD_ID_TO_KEY[accID])

	return None

def getEmapsID (emapsKey):
	_initialize()

	if EMAPS_KEY_TO_ID.has_key(emapsKey):
		return EMAPS_KEY_TO_ID[emapsKey]

	return None

def getEmapsIDByID (accID):
	_initialize()

	emapsKey = getEmapsKeyByID(accID)
	if emapsKey:
		return getEmapsID(emapsKey)

	return None

def getEmapsTerm (emapsKey):
	_initialize()

	if EMAPS_KEY_TO_TERM.has_key(emapsKey):
		return EMAPS_KEY_TO_TERM[emapsKey]
	return None

def getStageByKey (emapsKey):
	emapsID = getEmapsID(emapsKey)
	if emapsID:
		return emapsID[-2:]
	return None

def filterRows (rows, structureKeyIndex, name = ''):
	# filter the given set of rows down to a subset that contains only
	# the rows where the structure key can be mapped to an EMAPS key
	# (but does not actually do the mapping).  structureKeyIndex is the
	# index of the structureKey in each individual row.  name can be
	# specified to aid in debugging output.

	global FILTERED_SET

	FILTERED_SET = FILTERED_SET + 1
	if not name:
		name = 'set %d' % FILTERED_SET

	subset = []
	for row in rows:
		if getEmapsKey(row[structureKeyIndex]):
			subset.append(row)

	logger.debug ('Filtered %s from %d rows down to %d' % (name,
		len(rows), len(subset)) )

	return subset

def unload():
	# clear the global caches and restore this module to an unitialized
	# state (to save memory)

	global INITIALIZED, AD_ID_TO_KEY, EMAPS_KEY_TO_ID, EMAPS_ID_TO_KEY
	global AD_KEY_TO_EMAPS_KEY, OTHER_ID_TO_EMAPS_KEY, EMAPS_KEY_TO_TERM
	global FILTERED_SET

	INITIALIZED = False
	AD_ID_TO_KEY = {}
	EMAPS_KEY_TO_ID = {}
	EMAPS_ID_TO_KEY = {}
	AD_KEY_TO_EMAPS_KEY = {}
	OTHER_ID_TO_EMAPS_KEY = {}
	OTHER_ID_TO_EMAPS_KEY = {}
	FILTERED_SET = 0

	gc.collect()
	logger.debug('Cleared ADMapper caches')
	return

