# Module: GXDUtils.py
# Purpose: to provide handy utility functions for dealing with GXD data

import dbAgnostic
import logger
import gc

###--- functions dealing with ages ---###

# list of (old, new) pairs for use in seek-and-replace loops in abbreviate()
TIMES = [ (' day ',''), ('week ','w '), ('month ','m '), ('year ','y ') ]

# list of (old, new) pairs for use in seek-and-replace loops in abbreviate()
QUALIFIERS = [ ('embryonic', 'E'), ('postnatal', 'P') ]

ASSAY_TYPE_SEQMAP = None

def getAssayTypeSeq(assayType):
        """
        returns GXD specified order for assay types
        
        assayType can be either _assaytype_key or assaytype name
        
        """
        global ASSAY_TYPE_SEQMAP
        
        if ASSAY_TYPE_SEQMAP == None:
                query = "select assaytype, _assaytype_key, sequencenum from gxd_assaytype"
                (cols, rows) = dbAgnostic.execute(query)
                ASSAY_TYPE_SEQMAP = {}
                for row in rows:
                        # assaytype name
                       ASSAY_TYPE_SEQMAP[row[0].lower()] = row[2]
                       # _assaytype_key
                       ASSAY_TYPE_SEQMAP[row[1]] = row[2]
                       
                       
        # check assayType.lower()
        if not isinstance(assayType, int) \
            and not isinstance(assayType, long):
                assayType = assayType.lower()
                
        return assayType in ASSAY_TYPE_SEQMAP \
                and ASSAY_TYPE_SEQMAP[assayType] \
                or 999
                


def abbreviateAge (
        s               # string; specimen's age from gxd_expression.age
        ):
        # Purpose: convert 's' to a more condensed format for display on a
        #       query results page
        # Returns: string; with substitutions made as given in 'TIMES' and
        #       'QUALIFIERS' above.
        # Assumes: 's' contains at most one value from 'TIMES' and one value
        #       from 'QUALIFIERS'.  This is for efficiency, so we don't have
        #       to check every one for every invocation.
        # Effects: nothing
        # Throws: nothing

        # we have two different lists of (old, new) strings to check...
        for items in [ TIMES, QUALIFIERS ]:
                for (old, new) in items:

                        # if we do not find 'old' in 's', then we just go back
                        # up to continue the inner loop.

                        pos = s.find(old)
                        if pos == -1:
                                continue

                        # otherwise, we replace 'old' with 'new' and break out
                        # of the inner loop to go back to the outer one.

                        s = s.replace(old, new)
                        break
        return s

###--- functions dealing with EMAPA/EMAPS mapping ---###

EMAPS_TO_EMAPA = None	# dictionary mapping EMAPS key to EMAPA key
EMAPA_STAGE_RANGE = None	# dictionary mapping EMAPA key -> range string
EMAPA_TERM = None		# dictionary mapping EMAPA key -> term
EMAPA_START_STAGE = None	# dictionary mapping EMAPA key -> start stage
EMAPA_ID = None			# dictionary mapping EMAPA key -> acc ID

def getEmapaID (emapaKey):
	# get the primary accession ID for the EMAPA term with the given key

	global EMAPA_ID

	if EMAPA_ID == None:
		EMAPA_ID = {}

		query = '''select a.accID,
				t._Term_key
			from acc_accession a,
				voc_term t,
				voc_vocab v
			where a._MGIType_key = 13
				and a._Object_key = t._Term_key
				and t._Vocab_key = v._Vocab_key
				and v.name = 'EMAPA'
				and a.preferred = 1
				and a.private = 0'''

		(cols, rows) = dbAgnostic.execute(query)

		emapaCol = dbAgnostic.columnNumber (cols, '_Term_key')
		idCol = dbAgnostic.columnNumber (cols, 'accID')

		for row in rows:
			EMAPA_ID[row[emapaCol]] = row[idCol]

		logger.debug('Cached %d EMAPA IDs' % len(EMAPA_ID))

	if EMAPA_ID.has_key(emapaKey):
		return EMAPA_ID[emapaKey]
	return None

def getEmapaKey (emapsKey):
	# get the EMAPA key corresponding to the given EMAPS key

	global EMAPS_TO_EMAPA

	if EMAPS_TO_EMAPA == None:
		query = '''select _emapa_term_key, _term_key
			from voc_term_emaps'''

		(cols, rows) = dbAgnostic.execute(query)

		EMAPS_TO_EMAPA = {}

		emapsCol = dbAgnostic.columnNumber (cols, '_term_key')
		emapaCol = dbAgnostic.columnNumber (cols, '_emapa_term_key')

		for row in rows:
			EMAPS_TO_EMAPA[row[emapsCol]] = row[emapaCol]

		logger.debug('Mapped %d EMAPS terms to EMAPA' % \
			len(EMAPS_TO_EMAPA))

	if EMAPS_TO_EMAPA.has_key(emapsKey):
		return EMAPS_TO_EMAPA[emapsKey]
	return None

def getEmapaStageRange (emapaKey):
	# get the stage range (eg- "TS1-5") for the given EMAPA key

	global EMAPA_STAGE_RANGE

	if EMAPA_STAGE_RANGE == None:
		query = '''select _term_key, startStage, endStage
			from voc_term_emapa'''

		(cols, rows) = dbAgnostic.execute(query)

		EMAPA_STAGE_RANGE = {}

		emapaCol = dbAgnostic.columnNumber (cols, '_term_key')
		startCol = dbAgnostic.columnNumber (cols, 'startStage')
		endCol = dbAgnostic.columnNumber (cols, 'endStage')

		for row in rows:
			startStage = row[startCol]
			endStage = row[endCol]

			if startStage != endStage:
				s = 'TS%s-%s' % (startStage, endStage)
			else:
				s = 'TS%s' % startStage

			EMAPA_STAGE_RANGE[row[emapaCol]] = s

		logger.debug('Got stage ranges for %d EMAPA terms' % \
			len(EMAPA_STAGE_RANGE))

	if EMAPA_STAGE_RANGE.has_key(emapaKey):
		return EMAPA_STAGE_RANGE[emapaKey]
	return None

def getEmapaTerm (emapaKey):
	# get the structure term for the given EMAPA key

	global EMAPA_TERM

	if EMAPA_TERM == None:
		query = '''select t._term_key, t.term
			from voc_term t, voc_vocab v
			where t._vocab_key = v._vocab_key
				and v.name = 'EMAPA' '''

		(cols, rows) = dbAgnostic.execute(query)

		EMAPA_TERM = {}

		emapaCol = dbAgnostic.columnNumber (cols, '_term_key')
		termCol = dbAgnostic.columnNumber (cols, 'term')

		for row in rows:
			EMAPA_TERM[row[emapaCol]] = row[termCol]

		logger.debug('Got structures for %d EMAPA terms' % \
			len(EMAPA_TERM))

	if EMAPA_TERM.has_key(emapaKey):
		return EMAPA_TERM[emapaKey]
	return None

def getEmapaStartStage (emapaKey):
	# get the start stage (as an integer) for the given EMAPA key

	global EMAPA_START_STAGE

	if EMAPA_START_STAGE == None:
		query = 'select _term_key, startStage from voc_term_emapa'

		(cols, rows) = dbAgnostic.execute(query)

		EMAPA_STAGE_RANGE = {}

		emapaCol = dbAgnostic.columnNumber (cols, '_term_key')
		startCol = dbAgnostic.columnNumber (cols, 'startStage')

		EMAPA_START_STAGE = {}

		for row in rows:
			EMAPA_START_STAGE[row[emapaCol]] = \
				int(row[startCol])

		logger.debug('Got start stages for %d EMAPA terms' % \
			len(EMAPA_START_STAGE))

	if EMAPA_START_STAGE.has_key(emapaKey):
		return EMAPA_START_STAGE[emapaKey]
	return None

###--- functions dealing with identifying high-level ---###
###--- EMAPA terms for each lower-level EMAPA term   ---###

# maps from each EMAPA term key to a list of its high-level term keys, as
# { term key : [ high level term key, ... ]
TERM_TO_HIGH_LEVEL = None

# maps from each EMAPA high level term key to its start stage
# { term key : int start stage }
START_STAGE = {}

# maps from each EMAPA high level term key to its end stage
# { term key : int end stage }
END_STAGE = {}

def _getHighLevelTermKeys():
	# retrieves a list of term keys for high-level EMAPA terms, including
	# the children of "mouse" (except organ system) plus the children of
	# "organ system".  also populates globals START_STAGE and END_STAGE.

	# SQL command to retrieve the data set specified in comment above
	cmd = '''select distinct ct._Term_key, startStage, endStage
		from voc_vocab v, voc_term pt,
			dag_node pn, dag_edge e, dag_node cn,
			voc_term ct, dag_dag dd, voc_term_emapa a
		where v.name = 'EMAPA'
			and ct._Term_key = a._Term_key
			and v._Vocab_key = pt._Vocab_key
			and pt.term in ('mouse', 'organ system')
			and pt._Term_Key = pn._Object_key
			and pn._Node_key = e._Parent_key
			and e._Child_key = cn._Node_key
			and cn._Object_key = ct._Term_key
			and e._DAG_key = dd._DAG_key
			and ct.term not in ('mouse', 'organ system')
			and dd.name = 'EMAPA' '''

	(cols, rows) = dbAgnostic.execute(cmd)
	termCol = dbAgnostic.columnNumber (cols, '_Term_key')
	startCol = dbAgnostic.columnNumber (cols, 'startStage')
	endCol = dbAgnostic.columnNumber (cols, 'endStage')
	
	terms = []
	for row in rows:
		terms.append(row[termCol])
		START_STAGE[row[termCol]] = int(row[startCol])
		END_STAGE[row[termCol]] = int(row[endCol])

	logger.debug('Got %d high level EMAPA terms' % len(terms))
	return terms

def _cacheHighLevelMapping():
	# build a cache that maps from each EMAPA term key to its associated
	# high-level term keys

	global TERM_TO_HIGH_LEVEL

	TERM_TO_HIGH_LEVEL = {}
	highLevelTerms = _getHighLevelTermKeys()

	# initially fill in the high-level terms themselves

	for termKey in highLevelTerms:
		TERM_TO_HIGH_LEVEL[termKey] = [ termKey ]

	# note the high-level ancestors for each descendent term

	cmd = '''select distinct c._AncestorObject_key,
			c._DescendentObject_key
		from dag_closure c, dag_dag d
		where c._DAG_key = d._DAG_key
			and d.name = 'EMAPA'
			and c._AncestorObject_key in (%s)''' % (
				','.join(map(str, highLevelTerms)) )

	(cols, rows) = dbAgnostic.execute(cmd)
	ancestorCol = dbAgnostic.columnNumber (cols, '_AncestorObject_key')
	descendentCol = dbAgnostic.columnNumber (cols, '_DescendentObject_key')

	for row in rows:
		descendent = row[descendentCol]
		ancestor = row[ancestorCol]

		if TERM_TO_HIGH_LEVEL.has_key(descendent):
			TERM_TO_HIGH_LEVEL[descendent].append(ancestor)
		else:
			TERM_TO_HIGH_LEVEL[descendent] = [ ancestor ]

	logger.debug('Cached %d ancestors for %d EMAPA terms' % (
		len(rows), len(TERM_TO_HIGH_LEVEL)) )
	return

def getEmapaHighLevelTerms(emapaKey, stage):
	# return a list of high level terms and IDs for the term with the
	# given EMAPA key.  Each item in the returned list is a tuple:
	# (EMAPA ID, EMAPA term)

	if TERM_TO_HIGH_LEVEL == None:
		_cacheHighLevelMapping()

	if not TERM_TO_HIGH_LEVEL.has_key(emapaKey):
		return []

	intStage = int(stage)

	terms = []
	for ancestorKey in TERM_TO_HIGH_LEVEL[emapaKey]:
		# We only want to include this high level term if the result's
		# stage is within the term's stage range.

		startStage = START_STAGE[ancestorKey]
		endStage = END_STAGE[ancestorKey]

		if startStage <= intStage <= endStage: 
			terms.append( (getEmapaID(ancestorKey), 
				getEmapaTerm(ancestorKey)) )
	return terms

def unload():
	# remove cached items from memory, allowing the space to be reclaimed
	# and otherwise used

	global EMAPS_TO_EMAPA, EMAPA_STAGE_RANGE, EMAPA_TERM, EMAPA_ID
	global EMAPA_START_STAGE, TERM_TO_HIGH_LEVEL, START_STAGE, END_STAGE

	EMAPS_TO_EMAPA = None
	EMAPA_STAGE_RANGE = None
	EMAPA_TERM = None
	EMAPA_START_STAGE = None
	EMAPA_ID = None	
	TERM_TO_HIGH_LEVEL = None
	START_STAGE = {}
	END_STAGE = {}

	gc.collect()
	logger.debug('Reclaimed memory in GXDUtils')
	return

###--- functions and variables dealing with identifying genotypes ---###
###--- and genotype/assay pairs as being wild-type or not         ---###

# has two types of keys:
# - genotype key - for genotypes that are always wild-type (no allele pairs)
# - (genotype key, assay key) - for genotypes that can be considered wild-type for certain assay types
#    These pairs must have:
#       a. assay type = In situ reporter (knock in) (key 9)
#	    b. only one allele pair
#	    c. allele pair with one wild type allele and one other
#	    d. both alleles for the assayed gene
WILD_TYPE = None

def clearWildTypeCache():
    # return the WILD_TYPE global variable to its uninitialized state and free up its memory
    global WILD_TYPE

    WILD_TYPE = None
    gc.collect()
    logger.debug('Reclaimed memory in GXDUtils')
    return

def isWildType(genotypeKey, assayKey = None):
    # Returns 1 if either genotypeKey is always wild-type or if the (genotypeKey, assayKey) pair can
    # be considered wild-type.  Returns 0 if neither is true.
    
    global WILD_TYPE
    
    if WILD_TYPE == None:
        _cacheWildTypeData()
        
    if genotypeKey in WILD_TYPE:
        return 1
    if (genotypeKey, assayKey) in WILD_TYPE:
        return 1
    return 0

def _cacheWildTypeData():
    # populates the WILD_TYPE global variable per the comments above its definition
    global WILD_TYPE
    
    WILD_TYPE = {}

	# genotypes with no allele pairs (treat expression assays for these as wild type)
    alwaysWild = '''select g._Genotype_key
        from gxd_genotype g
        where g._Genotype_key >= 0
            and not exists (select 1 from gxd_allelepair p
                where g._Genotype_key = p._Genotype_key)'''

    (cols, rows) = dbAgnostic.execute(alwaysWild)
    for row in rows:
        WILD_TYPE[row[0]] = 1
    logger.debug('Cached %d always wild genotypes' % len(rows))
    
	# also treat expression assays for these genotypes as wild type; these have:
	#	a. assay type = In situ reporter (knock in) (key 9)
	#	b. only one allele pair
	#	c. allele pair with one wild type allele and one other
	#	d. both alleles for the assayed gene
    sometimesWild = '''select distinct s._Assay_key, s._Genotype_key
        from gxd_specimen s, gxd_assay a, gxd_allelegenotype gag1, gxd_allelegenotype gag2,
            all_allele a1, all_allele a2
        where s._Assay_key = a._Assay_key
            and a._AssayType_key = 9
            and s._Genotype_key >= 0
            and exists (select 1 from gxd_expression e
                where e._Assay_key = a._Assay_key
                    and e.isForGxd = 1)
            and not exists (select 1 from gxd_allelepair gap
                where s._Genotype_key = gap._Genotype_key
                    and gap.sequenceNum > 1)
            and s._Genotype_key = gag1._Genotype_key
            and gag1._Allele_key = a1._Allele_key
            and a1.isWildType = 1
            and s._Genotype_key = gag2._Genotype_key
            and gag2._Allele_key = a2._Allele_key
            and a2.isWildType = 0
            and a1._Marker_key = a._Marker_key
            and a2._Marker_key = a._Marker_key'''

    (cols, rows) = dbAgnostic.execute(sometimesWild)
    assayCol = dbAgnostic.columnNumber(cols, '_Assay_key')
    genotypeCol = dbAgnostic.columnNumber(cols, '_Genotype_key')

    for row in rows:
        WILD_TYPE[(row[0], row[1])] = 1
    logger.debug('Cached %d genotype/assay pairs' % len(rows))
    return