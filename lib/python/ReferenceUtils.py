# Module: ReferenceUtils.py
# Purpose: to provide handy utility functions for dealing with reference data

import dbAgnostic
import logger

###--- globals ---###


# refs key -> jnum ID
jnumCache = {}

###--- private functions ---###

def _populateJnumCache(tableName = None):
	# populate the global 'jnumCache' with J: number for each refs key

	global jnumCache

	if tableName != None:
		whereClause = '''and exists (select 1 from %s t
			where r._Object_key = t._Refs_key)''' % tableName
		tableMessage = tableName
	else:
		whereClause = ''
		tableMessage = '(all)'

	cmd = '''select r._Object_key as _Refs_key, r.accID as jnumID
		from acc_accession r
		where r._MGIType_key = 1
			and r.prefixPart = 'J:'
			and r._LogicalDB_key = 1
			and r.preferred = 1
		%s''' % whereClause

	(cols, rows) = dbAgnostic.execute(cmd)

	keyCol = dbAgnostic.columnNumber(cols, '_Refs_key')
	jnumCol = dbAgnostic.columnNumber(cols, 'jnumID')

	for row in rows:
		jnumCache[row[keyCol]] = row[jnumCol]


	logger.debug ('Cached %d J: numbers for table %s' % (len(jnumCache),
		tableMessage))
	return

###--- functions dealing with reference data ---###

def restrict(tableName):
	# if you'd like to restrict J: number collection to only those cited
	# in a certain table, call this first and specify the table name
	# (to save memory)

	_populateJnumCache(tableName)
	return

def getJnumID(refsKey):
	# get J: number ID for the given refs key

	if len(jnumCache) == 0:
		_populateJnumCache()

	if jnumCache.has_key(refsKey):
		return jnumCache[refsKey]

	return None
