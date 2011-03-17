# Module: PrivateRefSet.py
# Purpose: to provide an easy means to determine which references are
#	supposed to be de-emphasized in the WI (not appearing in counts,
#	not highlighted on a marker detail page, etc.)

import dbAgnostic
import logger

###--- Globals ---###

# stores both J: number and reference keys as keys of the dictionary, for
# references which should be de-emphasized
PRIVATE_REFS = {}

###--- Private Functions ---###

def _initialize():
	global PRIVATE_REFS

	PRIVATE_REFS = {}

	basicQuery = '''select acc.accID, br._Refs_key
		from BIB_Refs br, ACC_Accession acc
		where br._Refs_key = acc._Object_key
			and acc._MGIType_key = 1
			and acc.prefixPart = 'J:'
			and acc._LogicalDB_key = 1'''

	dataLoads = basicQuery + " and br.journal ilike 'database%'"
	personalCommunications = basicQuery + \
		" and br.journal ilike 'personal%'"
	genbankSubmissions = basicQuery + \
		" and br.journal ilike 'Genbank Submission'"
	books = basicQuery + " and br.refType = 'BOOK'"
	dataSubmissions = basicQuery + \
		" and br.journal ilike '%Data Submission%'"
	curatorialRefs = basicQuery + " and br.journal is null " + \
		" and br._ReviewStatus_key = 2"

	queries = [ 
		('data load', dataLoads),
		('personal communication', personalCommunications), 
		('genbank submission', genbankSubmissions),
		('book', books), 
		('direct data submission', dataSubmissions), 
		('curatorial', curatorialRefs)
		]

	for (title, query) in queries:
		(cols, rows) = dbAgnostic.execute (query)

		idCol = dbAgnostic.columnNumber (cols, 'accID')
		keyCol = dbAgnostic.columnNumber (cols, '_Refs_key')

		for row in rows:
			PRIVATE_REFS[row[idCol]] = 1
			PRIVATE_REFS[row[keyCol]] = 1
		logger.debug ('Found %d %s references' % (len(rows), title))
	return

###--- Functions ---###

def isPrivate (ref):
	# determine whether 'ref' (either a J: number of a _Refs_key) is
	# a reference that should be de-emphasized

	if not PRIVATE_REFS:
		_initialize()

	return PRIVATE_REFS.has_key(ref)
