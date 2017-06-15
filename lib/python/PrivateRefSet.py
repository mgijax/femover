# Module: PrivateRefSet.py
# Purpose: to provide an easy means to determine which references are
#	supposed to be de-emphasized in the WI (not appearing in counts,
#	not highlighted on a marker detail page, etc.)
#
# 06/15/2017	lec
#	- TR12250/LitTriage ; see also gather/reference_gatherer.py
#	perhaps gather/reference_gatherer.py should us this library
#

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

	query = '''select acc.accID, br._Refs_key
		from BIB_Citation_Cache br, ACC_Accession acc
                where br.referenceType in  
                ('External Resource', 'MGI Curation Record', 'MGI Data Load', 
                 'MGI Direct Data Submission', 'Personal Communication', 'Newsletter')
		and br._Refs_key = acc._Object_key
		and acc._MGIType_key = 1
		and acc.prefixPart = 'J:'
		and acc._LogicalDB_key = 1
		'''

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
