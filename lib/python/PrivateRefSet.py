# Module: PrivateRefSet.py
# Purpose: to provide an easy means to determine which references are
#       supposed to be de-emphasized in the WI (not appearing in counts,
#       not highlighted on a marker detail page, etc.)
#
# 06/15/2017    lec
#       - TR12250/LitTriage ; see also gather/reference_gatherer.py
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

        query = '''select jnumID, _Refs_key
                from BIB_Citation_Cache
                where referenceType in  
                ('External Resource', 'MGI Curation Record', 'MGI Data Load', 
                 'MGI Direct Data Submission', 'Personal Communication', 'Newsletter', 'Book')
                '''

        (cols, rows) = dbAgnostic.execute (query)

        idCol = dbAgnostic.columnNumber (cols, 'jnumID')
        keyCol = dbAgnostic.columnNumber (cols, '_Refs_key')

        for row in rows:
                PRIVATE_REFS[row[idCol]] = 1
                PRIVATE_REFS[row[keyCol]] = 1
        logger.debug ('Found %d references' % (len(rows)))

        return

###--- Functions ---###

def isPrivate (ref):
        # determine whether 'ref' (either a J: number of a _Refs_key) is
        # a reference that should be de-emphasized

        if not PRIVATE_REFS:
                _initialize()

        return ref in PRIVATE_REFS
