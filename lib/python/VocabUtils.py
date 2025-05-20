# Module: VocabUtils.py
# Purpose: to provide handy utility functions for dealing with vocabularies
#       and their terms

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

###--- functions dealing with vocabularies ---###

def getVocabularies():
        # get a list of (vocab key, vocab name) tuples, one for each vocabulary, order by name

        cmd = '''select _Vocab_key, name
                from voc_vocab
                order by name'''

        (cols, rows) = dbAgnostic.execute (cmd)

        keyCol = dbAgnostic.columnNumber (cols, '_Vocab_key')
        nameCol = dbAgnostic.columnNumber (cols, 'name')

        vocabularies = []

        for row in rows:
                vocabularies.append ( (row[keyCol], row[nameCol]) )

        return vocabularies

###--- functions dealing with terms ---###

def getKey(termID):
        # finds the term key associated with the given ID

        global idCache

        if termID not in idCache:
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

        if termKey == None:
                return None
        
        if termKey not in termCache:
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

        if termKey not in abbrevCache:
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

        if termKey in synonymCache:
                if synonymType in synonymCache[termKey]:
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

###--- functions dealing with header terms ---###

HEADER_TEMP_TABLE = None                        # name of header temp table

def getHeaderTermTempTable():
        # Purpose: build a temp table containing header keys for the CL, DO, GO, MP, and EMAPA vocabularies.
        # Returns: string; name of the temp table
        
        global HEADER_TEMP_TABLE

        if HEADER_TEMP_TABLE:
                return HEADER_TEMP_TABLE
        
        HEADER_TEMP_TABLE = 'term_headers'

        cmd0 = '''with headers as (
                        select distinct _Object_key, label
                        from mgi_setmember
                        where _Set_key in (1054, 1049, 1050, 1051, 1060)
                        )
                select v._Vocab_key, t._Term_key, v.name, t.term, t.abbreviation, h.label
                into temporary table %s
                from headers h, voc_term t, voc_vocab v
                where h._Object_key = t._Term_key
                        and t._Vocab_key = v._Vocab_key
                order by v.name, t.term''' % HEADER_TEMP_TABLE
        
        cmd1 = 'create index hht0 on %s (_Vocab_key)' % HEADER_TEMP_TABLE
        cmd2 = 'create index hht1 on %s (_Term_key)' % HEADER_TEMP_TABLE
        
        for cmd in [ cmd0, cmd1, cmd2 ]:
                dbAgnostic.execute(cmd)

        logger.debug('Built temp table: %s' % HEADER_TEMP_TABLE)
        return HEADER_TEMP_TABLE
