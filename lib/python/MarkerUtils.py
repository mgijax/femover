# Module: MarkerUtils.py
# Purpose: to provide handy utility functions for dealing with mouse marker
#    data

import dbAgnostic
import logger
import gc
import Lookup
import utils

###--- globals ---###

# marker key -> (genetic chrom, genomic chrom, start coord, end coord,
#    chromosome sequence number)
coordCache = {}    

# marker key -> organism
organismCache = {}

# organism -> organism key
organismKeyCache = {}

# marker key -> symbol
symbolCache = {}

# marker key -> primary ID cache
idCache = {}

# primary ID -> marker key cache
keyCache = {}

# acc ID -> marker key cache (for non-mouse markers)
nonMouseEGCache = None

# cache of rows for traditional allele-to-marker relationships.  Each row is:
#    [ allele key, marker key, count type, count type sequence num ]
amRows = None

# cache of rows for 'mutation involves' allele-to-marker relationships, each:
#    [ allele key, marker key, count type, count type sequence num ]
miRows = None

# cache of rows for 'expresses component' allele-to-marker relationships, each:
#    [ allele key, marker key, count type, count type sequence num ]
ecRows = None

# constants specifying which set of marker/allele pairs to return
TRADITIONAL = 'traditional'
MUTATION_INVOLVES = 'mutation_involves'
EXPRESSES_COMPONENT = 'expresses_component'
UNIFIED = 'unified'

# mapping from marker type key to marker type name
markerTypeLookup = Lookup.Lookup('MRK_Types', '_Marker_Type_key', 'name')

# marker key -> marker type key
markerTypeCache = {}

###--- private functions ---###

def _populateCoordCache():
    # populate the global 'coordCache' with location data for markers

    global coordCache

    cmd = '''select _Marker_key, genomicChromosome, chromosome,
            startCoordinate, endCoordinate, sequenceNum
        from mrk_location_cache
        where _Organism_key = 1'''

    (cols, rows) = dbAgnostic.execute(cmd)

    keyCol = dbAgnostic.columnNumber(cols, '_Marker_key')
    genomicChrCol = dbAgnostic.columnNumber(cols, 'genomicChromosome')
    geneticChrCol = dbAgnostic.columnNumber(cols, 'chromosome')
    startCol = dbAgnostic.columnNumber(cols, 'startCoordinate')
    endCol = dbAgnostic.columnNumber(cols, 'endCoordinate')
    seqNumCol = dbAgnostic.columnNumber(cols, 'sequenceNum')

    for row in rows:
        coordCache[row[keyCol]] = (row[geneticChrCol],
            row[genomicChrCol], row[startCol], row[endCol],
            row[seqNumCol])

    del cols
    del rows
    gc.collect()

    logger.debug ('Cached %d locations' % len(coordCache))
    return

def _populateOrganismCache():
        # populate the global 'organismCache' with organisms for all current
        # and pending markers

        global organismCache, organismKeyCache

        cmd = '''select m._Marker_key, o.commonName, o._Organism_key
                from mrk_marker m, mgi_organism o
                where m._Organism_key = o._Organism_key
                        and m._Marker_Status_key = 1'''

        (cols, rows) = dbAgnostic.execute(cmd)

        keyCol = dbAgnostic.columnNumber (cols, '_Marker_key')
        orgCol = dbAgnostic.columnNumber (cols, 'commonName')
        orgKeyCol = dbAgnostic.columnNumber (cols, '_Organism_key')

        for row in rows:
                organism = utils.cleanupOrganism(row[orgCol])
                organismCache[row[keyCol]] = organism
                organismKeyCache[organism] = row[orgKeyCol]

        del cols
        del rows
        gc.collect()

        logger.debug ('Cached %d marker organisms' % len(organismCache))
        logger.debug ('Cached %d organism keys' % len(organismKeyCache))
        return

def _populateSymbolCache():
        # populate the global 'symbolCache' with symbols for markers

    global symbolCache

    cmd = '''select _Marker_key, symbol
                from mrk_marker
                where _Marker_Status_key = 1'''

    (cols, rows) = dbAgnostic.execute(cmd)

    keyCol = dbAgnostic.columnNumber (cols, '_Marker_key')
    symbolCol = dbAgnostic.columnNumber (cols, 'symbol')

    for row in rows:
        symbolCache[row[keyCol]] = row[symbolCol]

    del cols
    del rows
    gc.collect()

    logger.debug ('Cached %d marker symbols' % len(symbolCache))
    return

def _populateIDCache():
    # populate the global 'idCache' with primary IDs for mouse markers

    global idCache

    cmd = '''
        select m._Marker_key, a.accID
        from mrk_marker m, acc_accession a
        where m._Organism_key = 1
            and m._Marker_Status_key = 1
            and m._Marker_key = a._Object_key
            and a._MGIType_key = 2
            and a.private = 0
            and a.preferred = 1
            and a._LogicalDB_key = 1
            and a.prefixPart = 'MGI:' 
	union
        select m._Marker_key, a.accID
        from mrk_marker m, acc_accession a
        where m._Organism_key != 1
            and m._Marker_Status_key = 1
            and m._Marker_key = a._Object_key
            and a._MGIType_key = 2
            and a.private = 0
            and a.preferred = 1
            and a._LogicalDB_key = 55
	'''

    (cols, rows) = dbAgnostic.execute(cmd)

    keyCol = dbAgnostic.columnNumber (cols, '_Marker_key')
    idCol = dbAgnostic.columnNumber (cols, 'accID')

    for row in rows:
        idCache[row[keyCol]] = row[idCol]

    del cols
    del rows
    gc.collect()

    logger.debug ('Cached %d marker IDs' % len(idCache))
    return

def _populateNonMouseEGCache():
    global nonMouseEGCache

    if nonMouseEGCache != None:
        return

    nonMouseEGCache = {}

    cmd = '''select m._Marker_key, a.accID
        from mrk_marker m, acc_accession a
        where m._Organism_key != 1
            and m._Marker_Status_key = 1
            and m._Marker_key = a._Object_key
            and a._MGIType_key = 2
            and a._LogicalDB_key = 55
            and a.private = 0'''

    (cols, rows) = dbAgnostic.execute(cmd)

    keyCol = dbAgnostic.columnNumber (cols, '_Marker_key')
    idCol = dbAgnostic.columnNumber (cols, 'accID')

    for row in rows:
        nonMouseEGCache[row[idCol]] = row[keyCol]

    del cols
    del rows
    gc.collect()

    logger.debug ('Cached %d non-mouse marker IDs' % len(nonMouseEGCache))
    return

def _populateMarkerTypeCache():
    # caches types for markers
    global markerTypeCache

    if len(markerTypeCache) > 0:
        return

    cmd1 = '''select _Marker_key, _Marker_Type_key
        from mrk_marker
        where _Marker_Status_key = 1'''

    (cols, rows) = dbAgnostic.execute(cmd1)

    markerCol = dbAgnostic.columnNumber (cols, '_Marker_key')
    typeCol = dbAgnostic.columnNumber (cols, '_Marker_Type_key')
    
    for row in rows:
        markerTypeCache[row[markerCol]] = row[typeCol]

    del cols
    del rows
    gc.collect()

    logger.debug('Cached %d marker types' % len(markerTypeCache))
    return 

def _populateMarkerAlleleCache():
    global amRows

    if amRows != None:
        return

    cmd1 = '''select distinct a._Marker_key,
            vt.term as countType,
            _Allele_key,
            vt.sequenceNum
        from all_allele a, voc_term vt
        where vt._Vocab_key = 38
            and vt._Term_key = a._Allele_Type_key
            and a.isWildType = 0
            and a._Marker_key is not null
            and exists (select 1 from mrk_marker m
                where a._Marker_key = m._Marker_key)
        order by a._Marker_key, vt.sequenceNum'''

    (cols1, rows1) = dbAgnostic.execute(cmd1)

    markerCol = dbAgnostic.columnNumber (cols1, '_Marker_key')
    alleleCol = dbAgnostic.columnNumber (cols1, '_Allele_key')
    typeCol = dbAgnostic.columnNumber (cols1, 'countType')
    seqNumCol = dbAgnostic.columnNumber (cols1, 'sequenceNum')

    amRows = []
    for row in rows1:
        amRows.append ( [ row[alleleCol], row[markerCol],
            row[typeCol], row[seqNumCol] ] )

    del cols1
    del rows1
    gc.collect()

    logger.debug ('Got %d traditional allele/marker pairs' % len(amRows))
    return 

def _getRelationships(typeName):
    # get a list of marker/allele pairs based on the given type of
    # relationships.

    cmd2 = '''select distinct r._Object_key_1 as allele_key,
            r._Object_key_2 as marker_key,
            t.term as countType,
            t.sequenceNum
        from mgi_relationship r,
            mgi_relationship_category c,
            all_allele a,
            voc_term t
        where c.name = '%s'
            and r._Category_key = c._Category_key
            and r._Object_key_1 = a._Allele_key
            and a._Allele_Type_key = t._Term_key''' % typeName

    (cols2, rows2) = dbAgnostic.execute(cmd2)

    markerCol = dbAgnostic.columnNumber (cols2, 'marker_key')
    alleleCol = dbAgnostic.columnNumber (cols2, 'allele_key')
    typeCol = dbAgnostic.columnNumber (cols2, 'countType')
    seqNumCol = dbAgnostic.columnNumber (cols2, 'sequenceNum')

    out = []
    for row in rows2:
        out.append ( [ row[alleleCol], row[markerCol], 
            row[typeCol], row[seqNumCol] ] ) 

    del cols2
    del rows2
    gc.collect()

    logger.debug ('Got %d %s allele/marker pairs' % (len(out), typeName))
    return out

def _populateMutationInvolvesCache():
    # populate the cache of marker/allele pairs based on
    # 'mutation_involves' relationships

    global miRows

    if miRows != None:
        return

    miRows = _getRelationships(MUTATION_INVOLVES)
    return

def _populateExpressesComponentCache():
    # populate the cache of marker/allele pairs based on
    # 'expresses_component' relationships

    global ecRows

    if ecRows != None:
        return

    ecRows = _getRelationships(EXPRESSES_COMPONENT)
    return

def _toHex(key):
    # convert integer 'key' to its hex equivalent as a string, but without
    # the '0x' prefix

    return hex(key)[2:]

def _bundle(alleleKey, markerKey):
    # bundle allele key and marker key into a unique string for use as a
    # dictionary key.  keys are converted to hex to save space.

    return _toHex(alleleKey) + ',' + _toHex(markerKey)

def _getMarkerAllelePairs(whichSet):
    # get a list of rows for allele/marker relationships, where each row is:
    #    [ allele key, marker key, count type, count type seq num ]
    # 'whichSet' should be one of TRADITIONAL, MUTATION_INVOVLES,
    # EXPRESSES_COMPONENT, or UNIFIED (which is the unique set of rows --
    # no duplicates)

    _populateMutationInvolvesCache()
    _populateExpressesComponentCache()
    _populateMarkerAlleleCache()

    if whichSet == TRADITIONAL:
        return amRows

    if whichSet == MUTATION_INVOLVES:
        return miRows

    if whichSet == EXPRESSES_COMPONENT:
        return ecRows

    unifiedList = []

    pairs = {}

    for myList in [ amRows, miRows, ecRows ]:
        for row in myList:
            pair = _bundle(row[0], row[1])    # allele + marker keys
            if pair not in pairs:
                unifiedList.append(row)
                pairs[pair] = 1

    logger.debug('Calculated set of %d distinct allele/marker pairs' % \
        len(unifiedList))

    del pairs
    gc.collect()

    return unifiedList

###--- functions dealing with location data ---###

def getMarkerCoords(markerKey):
    # get (genetic chrom, genomic chrom, start coord, end coord) for the
    # given marker key

    if len(coordCache) == 0:
        _populateCoordCache()

    if markerKey in coordCache:
        return coordCache[markerKey]

    return (None, None, None, None, 9999)

def getChromosome (marker):
    # get the chromosome for the given marker key, preferring
    # the genomic one over the genetic one

    (geneticChr, genomicChr, startCoord, endCoord, seqNum) = \
        getMarkerCoords(marker)

    if genomicChr:
        return genomicChr
    return geneticChr

def getChromosomeSeqNum (marker):
    # return the sequence number for sorting the chromosome of the given
    # marker key

    return getMarkerCoords(marker)[4]

def getStartCoord (marker):
    # return the start coordinate for the given marker key, or None if no
    # coordinates

    return getMarkerCoords(marker)[2]

def getEndCoord (marker):
    # return the end coordinate for the given marker key, or None if no
    # coordinates

    return getMarkerCoords(marker)[3] 

###--- functions dealing with accession IDs ---###

def getPrimaryID (markerKey):
    # return the primary MGI ID for the given mouse marker key, or None if
    # there is not one

    if len(idCache) == 0:
        _populateIDCache()

    if markerKey in idCache:
        return idCache[markerKey]
    return None

def getMarkerKey (primaryID):
    # return the marker key for the marker with the given primary MGI ID,
    # or None if there is not one

    global keyCache

    if len(keyCache) == 0:
        if len(idCache) == 0:
            _populateIDCache()

        keyCache = {}
        for key in list(idCache.keys()):
            keyCache[idCache[key]] = key

    primaryID = primaryID.strip()
    if primaryID in keyCache:
        return keyCache[primaryID]
    return None

def getNonMouseEGMarkerKey (accID):
    # return the marker key for the non-mouse marker with the given ID,
    # or None if there is not one

    global nonMouseEGCache

    if not nonMouseEGCache:
        _populateNonMouseEGCache()

    accID = accID.strip()
    if accID in nonMouseEGCache:
        return nonMouseEGCache[accID]
    return None

###--- functions dealing with nomenclature ---###

def getOrganismKey (markerKey):
        # return the _Organism_key for the marker's organism, or None if the
        # marker key is not for a current or pending marker

        org = getOrganism(markerKey)
        if (not org) or (org not in organismKeyCache):
                return None
        return organismKeyCache[org] 

def getOrganism (markerKey):
        # return the organism for the given marker key, or None if the key is
        # not for a current or pending marker

        if len(organismCache) == 0:
                _populateOrganismCache()

        if markerKey in organismCache:
                return organismCache[markerKey]
        return None

def getSymbol (markerKey):
        # return the symbol for the given marker key, or None if the key is
        # not for a mouse or human marker

    if len(symbolCache) == 0:
        _populateSymbolCache()

    if markerKey in symbolCache:
        return symbolCache[markerKey]
    return None

def getMarkerType (markerKey):
    # return marker type for the given marker key, or None if the key is
    # not for a mouse marker

    if len(markerTypeCache) == 0:
        _populateMarkerTypeCache()
    
    if markerKey in markerTypeCache:
        return markerTypeLookup.get(markerTypeCache[markerKey])
    return None

###--- functions dealing with allele counts ---###

def getAlleleCounts():
    # returns { marker key : count of all alleles }
    # includes both direct marker-to-allele relationships and ones from
    # 'mutation involves' and 'expresses component' relationships

    # each row has:
    # [ allele key, marker key, count type, count type order ]
    rows = _getMarkerAllelePairs(UNIFIED)

    alleles = {}        # alleles[markerKey] = [ allele keys ]

    for [ alleleKey, markerKey, countType, countTypeOrder ] in rows:
        if markerKey in alleles:
            if alleleKey not in alleles[markerKey]:
                alleles[markerKey].append(alleleKey)
        else:
            alleles[markerKey] = [ alleleKey ]

    counts = {}

    for markerKey in list(alleles.keys()):
        counts[markerKey] = len(alleles[markerKey])

    logger.debug('Found allele counts for %d markers' % len(counts))
    return counts

def getAlleleCountsByType():
    # returns two-item tuple with:
    #    { marker key : { count type : count of all alleles } }
    #    { count type sequence num : count type }
    # includes both direct marker-to-allele relationships and ones from
    # 'mutation involves' and 'expresses component' relationships

    # each row has:
    # [ allele key, marker key, count type, count type order ]
    rows = _getMarkerAllelePairs(UNIFIED)

    m = {}        # m[markerKey] = { count type : [ allele keys ] }
    c = {}        # c[count type seq num] = count type

    for [ alleleKey, markerKey, countType, countTypeOrder ] in rows:

        # We don't want to report counts for subsets of certain allele types.
        if countType in ('Not Applicable', 'Not Specified', 'Other'):
            continue
        
        # make sure we have the mapping from count type to its seq num
        if countTypeOrder not in c:
            c[countTypeOrder] = countType

        # track the alleles for each marker, separated by count type

        if markerKey not in m:
            m[markerKey] = { countType : [ alleleKey ] }

        elif countType not in m[markerKey]:
            m[markerKey][countType] = [ alleleKey ]

        elif alleleKey not in m[markerKey][countType]:
            m[markerKey][countType].append (alleleKey)

    counts = {}

    for markerKey in list(m.keys()):
        counts[markerKey] = {}

        for countType in list(m[markerKey].keys()):
            counts[markerKey][countType] = \
                len(m[markerKey][countType])

    logger.debug('Found %d types of allele counts for %d markers' % (
        len(c), len(counts)) )
    return counts, c

def getMutationInvolvesCounts():
    # returns dictionary:
    #    { marker key : count of alleles with that marker in a
    #        'mutation involves' relationship }

    rows = _getMarkerAllelePairs(MUTATION_INVOLVES)

    m = {}

    for [ alleleKey, markerKey, countType, countTypeOrder ] in rows:
        if markerKey not in m:
            m[markerKey] = [ alleleKey ]

        elif alleleKey not in m[markerKey]:
            m[markerKey].append (alleleKey)

    c = {}

    for markerKey in list(m.keys()):
        c[markerKey] = len(m[markerKey])

    logger.debug('Found %d markers with mutation involves relationships' \
        % len(c))
    return c

###--- functions to build temp tables for marker counts of pheno data ---###

# We need to produce temp tables which will aid the computation of these
# counts for each marker:
# 1. number of MP annotations rolled up to it, except "no phenotypic analysis"
# 2. number of its alleles in the genotypes for the source annotations of the
#    annotations in #1, except wild-type alleles
# 3. number of distinct strain names for those genotypes in #2
# 4. number of MP annotations (not in #1) which can be tied to the marker by
#    the traditional marker/allele route or by expresses component
#    relationships, except "no phenotypic analysis"

SRC_TABLE = None        # name of already-computed source annotation table
MA_TABLE = []           # list of names of already-computed tables of marker
                        # ...and allele pairs (multiples allowed for different
                        # ...combinations of traditional, EC, MI relationships)
MAG_TABLE = None        # name of already-computed table of marker, allele,
                        # ...genotype triples from SRC_TABLE
OA_TABLE = None         # name of already-computed table of markers and annot
                        # ...keys for annotations that did not roll up and get
                        # ...included in SRC_TABLE

mpg = 1002        # annotation type key for MP/Genotype
mpm = 1015        # annotation type for rolled-up MP/Marker annotations
npa = 293594        # term key for 'no phenotypic analysis'
mi = 1003        # relationship type for 'mutation involves'
ec = 1004        # relationship type for 'expresses component'

def getSourceAnnotationTable():
    # get the name of a temp table that has a mapping from a marker to its
    # rolled-up MP annotations and to the source annotations from which
    # they were derived.  Excludes "no phenotypic analysis" annotations.

    global SRC_TABLE
    
    if SRC_TABLE:
        return SRC_TABLE

    SRC_TABLE = 'source_annotations'

    cmd0 = '''select va._Object_key as _Marker_key,
            va._Annot_key as _DerivedAnnot_key,
            vep.value::int as _SourceAnnot_key
        into temporary table %s
        from VOC_Annot va,
            VOC_Evidence ve,
            VOC_Evidence_Property vep,
            VOC_Term t
        where va._AnnotType_key = %d
            and va._Annot_key = ve._Annot_key
            and ve._AnnotEvidence_key = vep._AnnotEvidence_key
            and vep._PropertyTerm_key = t._Term_key
            and va._Term_key != %d
            and t.term = '_SourceAnnot_key' ''' % (SRC_TABLE,
                mpm, npa)

    cmd1 = 'create index sa1 on %s (_Marker_key)' % SRC_TABLE
    cmd2 = 'cluster %s using sa1' % SRC_TABLE
    cmd3 = 'create index sa2 on %s (_DerivedAnnot_key)' % SRC_TABLE
    cmd4 = 'create index sa3 on %s (_SourceAnnot_key)' % SRC_TABLE
    cmd5 = 'select count(1) from %s' % SRC_TABLE

    for cmd in [ cmd0, cmd1, cmd2, cmd3, cmd4, cmd5 ]:
        cr = dbAgnostic.execute(cmd)

    logger.debug('Filled %s with %d rows' % (SRC_TABLE, cr[1][0][0]))

    return SRC_TABLE

def getMarkerAlleleTable(extras = [ ec ], tblName = 'ma_pairs'):
        # get the name of a temp table that has been populated with marker /
        # allele pairs, including via expresses component relationships (by
        # default).  Could pass in [ ec, mi ] to also include mutation involves
        # relationships.

    global MA_TABLE

    if tblName in MA_TABLE:
                return tblName

    MA_TABLE.append(tblName)

    cmd0 = '''create temporary table %s (
                _Marker_key int not null,
                _Allele_key int not null)''' % tblName

    cmd1 = '''insert into %s
                select _Marker_key, _Allele_key
                from all_allele
                where isWildType = 0
                        and _Marker_Key is not null
                union
                select _Object_key_2 as _Marker_key,
                        _Object_key_1 as _Allele_key
                from mgi_relationship
                where _Category_key in (%s)''' % (tblName,
                        ', '.join(map(str, extras)) )

    cmd2 = 'create index %s_p1 on %s (_Marker_key)' % (tblName, tblName)
    cmd3 = 'cluster %s using %s_p1' % (tblName, tblName)
    cmd4 = 'create index %s_p2 on %s(_Allele_key)' % (tblName, tblName)
    cmd5 = 'select count(1) from %s' % tblName

    for cmd in [ cmd0, cmd1, cmd2, cmd3, cmd4, cmd5 ]:
        cr = dbAgnostic.execute(cmd)

    logger.debug('Filled %s with %d rows' % (tblName, cr[1][0][0]))

    return tblName

def getSourceGenotypeTable():
    # get the name of a temp table that has been populated with markers,
    # alleles, and genotypes for the source annotations included in the
    # table built by getSourceAnnotations()

    global MAG_TABLE

    if MAG_TABLE:
        return MAG_TABLE

    MAG_TABLE = 'marker_allele_genotype'

    srcAnnot = getSourceAnnotationTable()
    pairs = getMarkerAlleleTable()

    cmd0 = '''select p._Marker_key, p._Allele_key, gag._Genotype_key
        into temporary table %s
        from %s s, voc_annot va,
            gxd_allelegenotype gag, %s p
        where s._SourceAnnot_key = va._Annot_key
            and va._Object_key = gag._Genotype_key
            and gag._Allele_key = p._Allele_key
            and p._Marker_key = s._Marker_key''' % (MAG_TABLE,
                srcAnnot, pairs)

    cmd1 = 'create index mag1 on %s (_Marker_key)' % MAG_TABLE
    cmd2 = 'cluster %s using mag1' % MAG_TABLE
    cmd3 = 'create index mag2 on %s (_Allele_key)' % MAG_TABLE
    cmd4 = 'create index mag3 on %s (_Genotype_key)' % MAG_TABLE
    cmd5 = 'select count(1) from %s' % MAG_TABLE

    for cmd in [ cmd0, cmd1, cmd2, cmd3, cmd4, cmd5 ]:
        cr = dbAgnostic.execute(cmd)

    logger.debug('Filled %s with %d rows' % (MAG_TABLE, cr[1][0][0]))

    return MAG_TABLE

def getOtherAnnotationsTable():
    # get the name of a temp table that has been popuplated with markers
    # and annotation keys for genotype-level annotations that did not roll
    # up to the marker (because of complex genotypes, etc.).  Excludes
    # "no phenotypic annotation" terms.

    global OA_TABLE

    if OA_TABLE:
        return OA_TABLE

    OA_TABLE = 'other_annotations'

    srcAnnot = getSourceAnnotationTable()
    pairs = getMarkerAlleleTable()

    cmd0 = '''select distinct p._Marker_key, va._Annot_key
                into temporary table other_annotations
                from voc_annot va, gxd_allelegenotype gag, %s p
                where va._AnnotType_key = %s
                        and not exists (select 1 from %s s
                                where va._Annot_key = s._SourceAnnot_key
                                and s._Marker_key = p._Marker_key
                                )
                        and va._Term_key != %s
                        and va._Object_key = gag._Genotype_key
                        and gag._Allele_key = p._Allele_key''' % (
                                pairs, mpg, srcAnnot, npa)

    cmd1 = 'create index oa1 on %s (_Marker_key)' % OA_TABLE
    cmd2 = 'cluster %s using oa1' % OA_TABLE
    cmd3 = 'create index oa2 on %s (_Annot_key)' % OA_TABLE
    cmd4 = 'select count(1) from %s' % OA_TABLE

    for cmd in [ cmd0, cmd1, cmd2, cmd3, cmd4 ]:
        cr = dbAgnostic.execute(cmd)

    logger.debug('Filled %s with %s rows' % (OA_TABLE, cr[1][0][0]))

    return OA_TABLE
