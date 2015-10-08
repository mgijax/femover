# Module: VocabUtils.py
# Purpose: to provide handy utility functions for dealing with vocabularies
#	and their terms

import dbAgnostic
import logger

###--- globals ---###

# term key -> term
termCache = {}

# term key -> abbreviation
abbrevCache = {}

# term key -> { synonym type : synonym }
synonymCache = {}

# acc ID -> term key
idCache = {}

###--- functions dealing with terms ---###

def getKey(termID):
	# finds the term key associated with the given ID

	global idCache

	if not idCache.has_key(termID):
		cmd = '''select _Object_key
			from acc_accession
			where _MGIType_key = 13
				and accID = '%s' ''' % termID

		(cols, rows) = dbAgnostic.execute(cmd)

		if len(rows) > 0:
			idCache[termID] = rows[0][0]
		else:
			idCache[termID] = None

	return idCache[termID]

def getTerm(termKey):
	# gets term associated with the given 'termKey', populating the cache
	# if needed; returns None if none exists

	global termCache

	if not termCache.has_key(termKey):
		cmd = '''select term
			from voc_term
			where _Term_key = %d''' % termKey

		(cols, rows) = dbAgnostic.execute(cmd)

		if len(rows) > 0:
			termCache[termKey] = rows[0][0]
		else:
			termCache[termKey] = None
	return termCache[termKey]

def getAbbrev(termKey):
	# gets abbreviation associated with the given 'termKey', populating
	# the cache if needed; returns None if none exists

	global abbrevCache

	if not abbrevCache.has_key(termKey):
		cmd = '''select abbreviation
			from voc_term
			where _Term_key = %d''' % termKey

		(cols, rows) = dbAgnostic.execute(cmd)

		if len(rows) > 0:
			abbrevCache[termKey] = rows[0][0]
		else:
			abbrevCache[termKey] = None
	return abbrevCache[termKey]

###--- functions dealing with synonyms ---###

def getSynonym(termKey, synonymType):
	# gets the synonym of the given type for the given term key,
	# populating the cache if needed; returns None if none exists

	global synonymCache

	if synonymCache.has_key(termKey):
		if synonymCache[termKey].has_key(synonymType):
			return synonymCache[termKey][synonymType]
	else:
		synonymCache[termKey] = {}

	cmd = '''select s.synonym
		from mgi_synonym s,
			mgi_synonymtype st
		where s._Object_key = %d
			and s._SynonymType_key = st._SynonymType_key
			and st._MGIType_key = 13
			and st.synonymType = '%s' ''' % (termKey, synonymType)

	(cols, rows) = dbAgnostic.execute(cmd)

	if len(rows) > 0:
		synonymCache[termKey][synonymType] = rows[0][0]
	else:
		synonymCache[termKey][synonymType] = None

	return synonymCache[termKey][synonymType]
