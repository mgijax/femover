#!/usr/local/bin/python
# 
# gathers data for the 'marker_mp_annotation' tables in the front-end database

import Gatherer
import logger
import MarkerUtils
import KeyGenerator
import re
import symbolsort
import gc
import dbAgnostic
import TagConverter

###--- Globals ---###

# temp table for source annotations of rolled-up MP annotations
# fields: _Marker_key, _DerivedAnnot_key, _SourceAnnot_key
SOURCE_ANNOT_TABLE = MarkerUtils.getSourceAnnotationTable()

# temp table for markers, alleles, and genotypes involved in rolled-up MP
# annotations
# fields: _Marker_key, _Allele_key, _Genotype_key
GENOTYPE_TABLE = MarkerUtils.getSourceGenotypeTable()

# temp table for MP annotations that did not roll up to markers
# fields: _Marker_key, _Annot_key
OTHER_ANNOT_TABLE = MarkerUtils.getOtherAnnotationsTable()

# KeyGenerators for the three tables we're producing
GENOTYPE_KG = KeyGenerator.KeyGenerator('marker_mp_genotype')
ANNOT_KG = KeyGenerator.KeyGenerator('marker_mp_annotation')
REFS_KG = KeyGenerator.KeyGenerator('marker_mp_annotation_reference')

# dictionary; maps from annotated term key to a list of its header term keys
HEADERS = {}

# top-levevl MP terms which should be excluded from consideration as
# slimgrid headers
EXCLUDE = [ 'no phenotypic analysis', 'normal phenotype', 'other phenotype' ]

###--- Functions ---###

def cacheHeaders():
    # cache a dictionary with the annotated term keys mapped to their
    # corresponding MP header term keys.

    global HEADERS

    # Top of the union includes the MP header terms for each annotated
    # term (where those are not annotations directly to a header term).
    # Bottom includes annotations to the header terms themselves.

    cmd = '''with mp_headers as (
        select distinct vah._Term_key
        from voc_annotheader vah, voc_term t
        where vah._AnnotType_key = 1002
            and vah._Term_key = t._Term_key
            and t.term not in ('%s')
        
        ), 
        mp_dag as (
        select dc._AncestorObject_key, dc._DescendentObject_key
        from dag_closure dc, mp_headers h
        where dc._AncestorObject_key = h._Term_key
        union
        select _Term_key, _Term_key
        from mp_headers
        )
        select distinct va._Term_key, dag._AncestorObject_key
        from voc_annot va, mp_dag dag
        where va._AnnotType_key = 1002
            and va._Term_key = dag._DescendentObject_key''' % \
                "', '".join(EXCLUDE)

    cols, rows = dbAgnostic.execute(cmd)

    HEADERS = {}

    termCol = Gatherer.columnNumber (cols, '_Term_key')
    headerCol = Gatherer.columnNumber (cols, '_AncestorObject_key')

    for row in rows:
        termKey = row[termCol]
        headerKey = row[headerCol]

        if HEADERS.has_key(termKey):
            HEADERS[termKey].append(headerKey)
        else:
            HEADERS[termKey] = [ headerKey ]

    logger.debug('Cached headers for %d terms' % len(HEADERS))

    del rows
    del cols
    gc.collect()

    return HEADERS

def getOtherGenotypeInfo(genotypeKey, genotypes):
	# If the allele cache loads haven't been run, then we may be missing
	# allele combinations.  This method finds the needed info for genotypes
	# with missing info, so this gatherer doesn't completely fail.

	# find what the current highest genotype sequence number is and 
	# increment it for this genotype
	mySeqNum = max(map(lambda x : x[2], genotypes.values())) + 1

	# assume we're now down to 1 note chunk per combination
	cmd1 = '''select distinct mn._Object_key as _Genotype_key, mnc.note,
			ps.strain, mnc.sequenceNum, a.accid
		from gxd_genotype gg
		left outer join mgi_note mn on (mn._MGIType_key = 12
		 	and gg._Genotype_key = mn._Object_key)
		left outer join mgi_notechunk mnc on (
			mn._Note_key = mnc._Note_key)
		left outer join mgi_notetype mnt on (
			mn._NoteType_key = mnt._NoteType_key
			and mnt.noteType = 'Combination Type 3')
		left outer join prb_strain ps on (
			gg._Strain_key = ps._Strain_key)
		left outer join acc_accession a on (
			gg._Genotype_key = a._Object_key
			and a._MGIType_key = 12
			and a._LogicalDB_key = 1)
		where gg._Genotype_key = %d
		order by mn._Object_key, mnc.sequenceNum''' % genotypeKey

    	cols1, rows1 = dbAgnostic.execute(cmd1)

	noteCol = Gatherer.columnNumber(cols1, 'note')
	strainCol = Gatherer.columnNumber(cols1, 'strain')
	idCol = Gatherer.columnNumber(cols1, 'accid')

	note = None
	strain = None
	accid = None

	for row in rows1:
		note = row[noteCol]
		strain = row[strainCol]
		accid = row[idCol]

	if note == None:
		cmd2 = '''select gap._Genotype_key, a1.symbol as symbol1,
				a2.symbol as symbol2
			from gxd_allelepair gap	
			left outer join all_allele a1 on (
				gap._Allele_key_1 = a1._Allele_key)
			left outer join all_allele a2 on (
				gap._Allele_key_2 = a2._Allele_key)
			where gap._Genotype_key = %d
			order by gap.sequenceNum''' % genotypeKey

	    	cols2, rows2 = dbAgnostic.execute(cmd2)

		symbol1col = Gatherer.columnNumber(cols2, 'symbol1')
		symbol2col = Gatherer.columnNumber(cols2, 'symbol2')

		note = ''

		for row in rows2:
			symbol1 = row[symbol1col]
			symbol2 = row[symbol2col]

			if len(note) > 0:
				note = note + '\n'

			if symbol1:
				note = note + symbol1
			else:
				note = note + '?'

			if symbol2:
				note = note + '/' + symbol2
			else:
				note = note + '/?'

	genotypes[genotypeKey] = (note, strain, mySeqNum, accid)
	return genotypes[genotypeKey]

# Due to special regex meaning of the pipe (|) symbol, we're going to convert
# them to be @ signs instead.
alleleRegex = re.compile('Allele.[^@]+@([^@]+)@')

def extractSymbols (note):
    # extract the allele symbols from the given 'note', containing the
    # allele pairs for a genotype; return as a comma-delimited string

    n = note.replace('|', '@')
    match = alleleRegex.search(n)
    symbols = ''

    while match:
        symbols = symbols + match.group(1) + ','
        match = alleleRegex.search(n, match.regs[0][1])

    return symbols

def compareGenotypes (a, b):
    # compare two tuples for sorting genotypes; each is:
    #    (allele symbols, background strain, genotype key)

    # sort by comma-delimited string of allele symbols
    x = symbolsort.nomenCompare(a[0], b[0])
    if x != 0:
        return x

    # if those match, sort by background strain
    y = symbolsort.nomenCompare(a[1], b[1])
    if y != 0:
        return y

    # if those match, sort by genotype key
    return cmp(a[2], b[2])

def compareAnnotations (a, b):
    # Compare two annotation rows for a marker.  Each row is a list
    # containing:
    # [ marker key, multigenic flag, qualifier, term, term key,
    #   term ID, annotation key, genotype key, reference key,
    #   jnum ID, numeric part of jnum ]
    # Basic rules:
    # 0. sort by marker key
    # 1. sort by genotype key
    # 2. sort by term
    # 3. if terms match, sort by qualifier
    # 4. fall back on sorting by annotation key, just to be deterministic

    v = cmp(a[0], b[0])
    if v != 0:
        return v

    w = cmp(a[7], b[7])
    if w != 0:
        return w

    x = symbolsort.nomenCompare(a[3], b[3])
    if x != 0:
        return x

    y = 0
    if a[2] != None:
        if b[2] != None:
            y = symbolsort.nomenCompare(a[2], b[2])
            if y != 0:
                return y 
        else:
            return 1

    elif b[2] != None:
        # prefer None qualifier to text qualifier
        return -1

    return cmp(a[6], b[6])

###--- Classes ---###

class MarkerMpAnnotationGatherer (Gatherer.CachingMultiFileGatherer):
    # Is: a data gatherer for the marker MP annotation tables
    # Has: queries to execute against the source database
    # Does: queries the source database for primary data for MP
    #    annotations for markers, collates results, writes
    #    tab-delimited text files

    def cacheGenotypes (self):
        # build a cache of genotype data for the current slice of
        # markers and return it as a dictionary:
        # { genotype key : (allele pairs, strain, sequence num) }

        cols, rows = self.results[0]

        # genotype key -> allele pairs
        alleles = {}

        # genotype key -> strain name
        strain = {}

        # genotype key -> acc ID
        accID = {}

        keyCol = Gatherer.columnNumber (cols, '_Genotype_key')
        noteCol = Gatherer.columnNumber (cols, 'note')
        strainCol = Gatherer.columnNumber (cols, 'strain')
        idCol = Gatherer.columnNumber (cols, 'acc_id')

        for row in rows:
            genotypeKey = row[keyCol]
            chunk = row[noteCol]

            accID[genotypeKey] = row[idCol]

            # There can currently be multiple note chunks per
            # genotype, if there are many allele pairs.  Collect
            # the multiples by concatenating them.

            if not alleles.has_key(genotypeKey):
                alleles[genotypeKey] = chunk
            else:
                alleles[genotypeKey] = alleles[genotypeKey] \
                    + chunk

            # if multiple note chunks, then the strain is simply
            # repeated with each row, so just store it once

            if not strain.has_key(genotypeKey):
                strain[genotypeKey] = row[strainCol]

        # We need to generate the sequence numbers for the genotypes,
        # so we need to sort them alphanumerically based on allele
        # symbols and background strains.  (So we'll need to extract
        # the allele symbols from the collected note chunks, append
        # the corresponding strain, sort them, and apply the sequence
        # numbers.)

        sortable = []

        for genotypeKey in alleles.keys():
            sortable.append( [
                extractSymbols(alleles[genotypeKey]),
                strain[genotypeKey],
                genotypeKey
                ] )

        sortable.sort(compareGenotypes)
        genoSeqNum = 0
        genotypes = {}

        for (symbols, strain, genotypeKey) in sortable:
            genoSeqNum = genoSeqNum + 1

            genotypes[genotypeKey] = (alleles[genotypeKey],
                strain, genoSeqNum, accID[genotypeKey])

        self.results[0] = (cols, [])
        del sortable
        del strain
        del alleles
        gc.collect()

        logger.debug('Cached %d genotypes' % len(genotypes))
        return genotypes 

    def processMarkersRows (self, rows, genotypes):
        # process this list of 'rows' for a single marker, assigning
        # any necessary sequence numbers and splitting the data up
        # into the appropriate output files.
        #
        # Each row is a list containing:
        # [ marker key, multigenic flag, qualifier, term, term key,
        #   term ID, annotation key, genotype key, reference key,
        #   jnum ID, numeric part of jnum ]
        #
        # genotypes is a dictionary:
        # { genotype key : (allele pairs, strain, sequence num, accID)

        # need to sort the annotations for the marker
        rows.sort(compareAnnotations)

        annotSeqNum = 0        # for ordering annotations

        lastMarker = None    # last marker key
        lastGenotype = None    # last genotype key
        lastQualifier = None    # last qualifier
        lastTerm = None        # last annotated term
        lastRef = None        # last reference key
        lastAnnot = None    # last annotation key
        lastMpGenoKey = None    # last mpGenotypeKey

        # dictionary to track which mpRefsKey values we've already
        # added to the file (db table), so we don't add dups
        mpRefsKeys = {}

        for [ markerKey, multigenic, qualifier, term, termKey,
            termID, annotKey, genotypeKey, refsKey, jnumID,
            numericPart ] in rows:

            # different marker or different genotype requires a
            # new mpGenoKey.  Add it to the file and update which
            # mpGenoKey we're using for associations

            if (lastMarker != markerKey) or \
                (lastGenotype != genotypeKey):

                mpGenoKey = GENOTYPE_KG.getKey( (markerKey,
                    genotypeKey) )

		if genotypes.has_key(genotypeKey):
	                (allelePairs, strain, genoSeqNum, accID) = \
        	            genotypes[genotypeKey]
		else:
			allelePairs, strain, genoSeqNum, accID = \
			    getOtherGenotypeInfo(genotypeKey, genotypes)

                self.addRow (self.genoTable, [ mpGenoKey,
                    markerKey, multigenic,
                    TagConverter.convert(allelePairs, False),
                    strain, accID, genoSeqNum ] )

                lastMarker = markerKey
                lastGenotype = genotypeKey
                
            # different mpGenoKey, qualifier, or term requires a
            # new mpAnnotKey (if same marker, genotype, qualifier, 
            # and term, use same mpAnnotKey, even if different
            # annotation key in production db).  If new, add it to 
            # the file and update which mpAnnotKey we're using for
            # associations

            if (lastMpGenoKey != mpGenoKey) \
                or (lastQualifier != qualifier) \
                or (lastTerm != term):

                mpAnnotKey = ANNOT_KG.getKey ( (mpGenoKey,
                    qualifier, termKey) )
                annotSeqNum = annotSeqNum + 1

                self.addRow (self.annotTable, [ mpAnnotKey,
                    mpGenoKey, qualifier, termKey, termID,
                    term, annotSeqNum ] )
                
                # Since we found a new annotation, we need to
                # record the headers for it.

                if HEADERS.has_key(termKey):
                    for headerKey in HEADERS[termKey]:
                        self.headerRowNum = 1 + \
                            self.headerRowNum
                        self.addRow (self.headerTable,
                            [ self.headerRowNum,
                                mpAnnotKey,
                                headerKey ] )
                lastTerm = term
                lastQualifier = qualifier

                # now that we've used the lastGenotype in the
                # prior comparison, we can update it to the
                # new one

                lastGenotype = genotypeKey
                lastMpGenoKey = mpGenoKey

            # If we have a new reference key or a new annotation
            # key, we need to add this reference.

# need to ensure we don't duplicate mpRefsKey values -- could happen for consecutive annotation keys where the first has multiple refs.  Keep a dictionary within this method to track which ones have been seen before.

            if (lastRef != refsKey) or (lastAnnot != mpAnnotKey):
                mpRefsKey = REFS_KG.getKey ( (mpAnnotKey,
                    refsKey) )

                if not mpRefsKeys.has_key(mpRefsKey):
                    self.addRow (self.refsTable, [ mpRefsKey,
                    mpAnnotKey, refsKey, jnumID,
                    numericPart ] )

                    mpRefsKeys[mpRefsKey] = 1

                lastRef = refsKey
                lastAnnot = mpAnnotKey
        return

    def processAnnotations (self, genotypes):
        # process the rows of annotations for this slice of markers;
        # genotypes is a dictionary:
        #    { genotype key : (allele pairs, strain, sequence num)

        cols, rows = self.results[1]

        markerCol = Gatherer.columnNumber(cols, '_Marker_key')
        annotCol = Gatherer.columnNumber(cols, '_Annot_key')
        multigenicCol = Gatherer.columnNumber(cols, 'multigenic')
        genotypeCol = Gatherer.columnNumber(cols, '_Genotype_key')
        qualifierCol = Gatherer.columnNumber(cols, 'qualifier')
        termKeyCol = Gatherer.columnNumber(cols, '_Term_key')
        termCol = Gatherer.columnNumber(cols, 'term')
        termIDCol = Gatherer.columnNumber(cols, 'term_id')
        refsKeyCol = Gatherer.columnNumber(cols, '_Refs_key')
        jnumCol = Gatherer.columnNumber(cols, 'jnum_id')
        numericPartCol = Gatherer.columnNumber(cols, 'numeric_part')

        # assumes rows are sorted by marker key, annotation key, and
        # the numeric part of the reference ID

        # The three files we produce each have a sequence number:
        # 1. genotype -- seq num is included in 'genotypes' cache
        # 2. annotation -- need to sort by term and assign seq num
        # 3. reference -- numeric part of J: num is in 'rows'

        rowsForMarker = []
        lastMarkerKey = None
        markerCount = 0

        for row in rows:
            markerKey = row[markerCol]
            annotKey = row[annotCol]
            multigenic = row[multigenicCol]
            genotypeKey = row[genotypeCol]
            qualifier = row[qualifierCol]
            termKey = row[termKeyCol]
            term = row[termCol]
            termID = row[termIDCol]
            refsKey = row[refsKeyCol]
            jnumID = row[jnumCol]
            numericPart = row[numericPartCol]

            if lastMarkerKey == None:
                # first trip through the loop
                lastMarkerKey = markerKey

            elif lastMarkerKey != markerKey:
                # finished collecting rows for 'lastMarker',
                # must now process those rows and start
                # collecting for the new 'markerKey'

                self.processMarkersRows(rowsForMarker,
                    genotypes)

                rowsForMarker = []
                lastMarkerKey = markerKey
                markerCount = markerCount + 1

                # every 100th marker, do garbage collection
                if markerCount % 100 == 0:
                    gc.collect()

            # go ahead and collect this row's data
            rowsForMarker.append ( [ markerKey, multigenic,
                qualifier, term, termKey, termID, annotKey, 
                genotypeKey, refsKey, jnumID, numericPart ] )

        self.processMarkersRows(rowsForMarker, genotypes)
        markerCount = markerCount + 1

        logger.debug('Processed %d rows for %d markers' % (len(rows),
            markerCount))
        return

    def collateResults (self):
        # handle database returns for one chunk of marker keys

        if not self.initialized:
            self.genoTable = 'marker_mp_genotype'
            self.annotTable = 'marker_mp_annotation'
            self.refsTable = 'marker_mp_annotation_reference'
            self.headerTable = 'marker_mp_annotation_to_header'
            self.headerRowNum = 0
            self.initialized = True

        genotypes = self.cacheGenotypes()
        self.processAnnotations(genotypes)
        return

###--- globals ---###

cmds = [
    # 0. get genotype data (allele combinations and strain names) for the
    # genotypes associated with the markers (either rolled-up or other
    # annotations)
    '''with markers as (select _Marker_key
        from mrk_marker
        where _Marker_key >= %d
        and _Marker_key < %d
        and _Organism_key = 1
        and _Marker_Status_key in (1,3)
        ),
    genotypes as (
        select mag._Marker_key, mag._Genotype_key
        from ''' + GENOTYPE_TABLE + ''' mag, markers m
        where mag._Marker_key = m._Marker_key
        union
        select a._Marker_key, va._Object_key as _Genotype_key
        from ''' + OTHER_ANNOT_TABLE + ''' a, voc_annot va, markers m
        where a._Annot_key = va._Annot_key
        and a._Marker_key = m._Marker_key
        ),
    acc_ids as (
        select g._Genotype_key, a.accID as acc_id
        from genotypes g, acc_accession a
        where g._Genotype_key = a._Object_key
            and a.preferred = 1
            and a._LogicalDB_key = 1
            and a._MGIType_key = 12
        )
    select distinct mn._Object_key as _Genotype_key, mnc.note, ps.strain,
        mnc.sequenceNum, a.acc_id
    from mgi_note mn, mgi_notechunk mnc, mgi_notetype mnt, genotypes g,
        gxd_genotype gg, prb_strain ps, acc_ids a
    where g._Genotype_key = mn._Object_key
        and g._Genotype_key = gg._Genotype_key
        and gg._Strain_key = ps._Strain_key
        and mn._MGIType_key = 12
        and mn._NoteType_key = mnt._NoteType_key
        and mnt.noteType = 'Combination Type 3'
        and mn._Note_key = mnc._Note_key
        and g._Genotype_key = a._Genotype_key
    order by mn._Object_key, mnc.sequenceNum''',    

    # 1. get the annotations and their references for the set of markers
    # (either rolled-up or other annotations)
    '''with markers as (select _Marker_key
        from mrk_marker
        where _Marker_key >= %d
        and _Marker_key < %d
        and _Organism_key = 1
        and _Marker_Status_key in (1,3)
        ),
    annotations as (
        select a._Marker_key,
            a._SourceAnnot_key as _Annot_key,
            0 as multigenic
        from ''' + SOURCE_ANNOT_TABLE + ''' a, markers m
        where a._Marker_key = m._Marker_key
        union
        select a._Marker_key,
            a._Annot_key,
            1 as multigenic
        from ''' + OTHER_ANNOT_TABLE + ''' a, markers m
        where a._Marker_key = m._Marker_key
        )
    select distinct s._Marker_key, s._Annot_key, s.multigenic,
        va._Object_key as _Genotype_key, q.term as qualifier,
        va._Term_key, t.term, aa.accID as term_id,
        ve._Refs_key, c.jnumid as jnum_id,
        c.numericPart as numeric_part
    from annotations s, voc_annot va, voc_term q, voc_term t,
        acc_accession aa, voc_evidence ve, bib_citation_cache c
    where s._Annot_key = va._Annot_key
        and va._Qualifier_key = q._Term_key
        and va._Term_key = t._Term_key
        and va._Term_key = aa._Object_key
        and aa._MGIType_key = 13
        and aa.preferred = 1
        and s._Annot_key = ve._Annot_key
        and ve._Refs_key = c._Refs_key
        and aa.preferred = 1
    order by s._Marker_key, s._Annot_key, c.numericPart''',    
    ]

files = [
    ('marker_mp_genotype',
        [ 'mp_genotype_key', 'marker_key', 'is_multigenic',
            'allele_pairs', 'strain', 'acc_id', 'sequence_num' ],
        [ 'mp_genotype_key', 'marker_key', 'is_multigenic',
            'allele_pairs', 'strain', 'acc_id', 'sequence_num' ]),
    ('marker_mp_annotation',
        [ 'mp_annotation_key', 'mp_genotype_key', 'qualifier',
            'term_key', 'term_id', 'term', 'sequence_num' ],
        [ 'mp_annotation_key', 'mp_genotype_key', 'qualifier',
            'term_key', 'term_id', 'term', 'sequence_num' ]),
    ('marker_mp_annotation_reference',
        [ 'mp_reference_key', 'mp_annotation_key', 'reference_key',
            'jnum_id', 'sequence_num' ],
        [ 'mp_reference_key', 'mp_annotation_key', 'reference_key',
            'jnum_id', 'sequence_num' ]),
    ('marker_mp_annotation_to_header',
        [ 'unique_key', 'mp_annotation_key', 'term_key' ],
        [ 'unique_key', 'mp_annotation_key', 'term_key' ]),
    ]

# global instance of a MarkerMpAnnotationGatherer
gatherer = MarkerMpAnnotationGatherer (files, cmds)
gatherer.setupChunking (
    '''select min(_Marker_key)
        from mrk_marker
        where _Organism_key = 1
            and _Marker_Status_key in (1,3)''',
    '''select max(_Marker_key)
        from mrk_marker
        where _Organism_key = 1
            and _Marker_Status_key in (1,3)''',
    )
gatherer.initialized = False
cacheHeaders()

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
    Gatherer.main (gatherer)
