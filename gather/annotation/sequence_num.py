#
# Module for generating the sequence nums for the annotation_gatherer
#
#

import AlleleAndGenotypeSorter
import VocabSorter
import dbAgnostic
import symbolsort

import logger
import tempfile
import transform
import constants as C
import go_isoforms


def _createSequenceNumTables():
    """
    Pre-compute all the data used for sorting annotations
    """
    
    logger.debug("creating sequencenum temp tables")
    
    # by_vocab sort
    logger.debug("creating vocab seqnum temp tables")
    _createVocabSequenceNums()
    
    # by_annottype sort
    logger.debug("creating annottype seqnum temp tables")
    _createAnnotTypeSequenceNums()
    
    # by_term sort
    logger.debug("creating term alpha seqnum temp tables")
    _createTermAlphaSequenceNums()

    # by_vocab_dag sort
    logger.debug("creating dag structure seqnum temp tables")
    _createVocabDagSequenceNums()
    logger.debug("creating vocab dag seqnum temp tables")

    # by_dag_structure sort
    _createDagStructureSequenceNums()

    # by_object_dag sorts
    logger.debug("creating object type + dag seqnum temp tables")
    _createObjectDagSequenceNums()
    
    # by_isoform sort
    logger.debug("creating isoform seqnum temp tables")
    _createIsoformSequenceNums()
    
    
    logger.debug("done creating sequencenum temp tables")
    
    
def _createVocabSequenceNums():
    """
    Create the sequence num data by vocab name
    
    Adds to VOCAB_SORT_TEMP_TABLE
    """
    cmd = '''
    select 
    row_number() over (order by name) sequencenum,
    name as vocab
    into temp %s
    from voc_vocab
    order by name
    ''' % C.VOCAB_SORT_TEMP_TABLE
    
    dbAgnostic.execute(cmd)
    

def _createAnnotTypeSequenceNums():
    """
    Create the sequence num data by annottype
    
    Adds to ANNOTTYPE_SORT_TEMP_TABLE
    """
    cmd = '''
    select 
    row_number() over (order by name) sequencenum,
    name as annottype
    into temp %s
    from voc_annottype
    order by name
    ''' % C.ANNOTTYPE_SORT_TEMP_TABLE
    
    dbAgnostic.execute(cmd)
    
    
def _createTermAlphaSequenceNums():
    """
    Create the sequence num data by term alpha
    
    Adds to TERM_SORT_TEMP_TABLE
    """
    cmd = '''
    select 
    row_number() over (order by t.term) sequencenum,
    t._term_key,
    va._annot_key        -- _annot_key for batch fetching
    into temp %s
    from voc_term t 
    join voc_annot va on
        va._term_key = t._term_key
    order by t.term
    ''' % C.TERM_SORT_TEMP_TABLE
    
    dbAgnostic.execute(cmd)
    
    cmd = '''create index tmp_term_sort_idx on %s (_annot_key)''' % C.TERM_SORT_TEMP_TABLE
    dbAgnostic.execute(cmd)

    
def _createVocabDagSequenceNums():
    """
    Create the sequence num data by vocab dag
    
    Adds to VOCAB_DAG_SORT_TEMP_TABLE
    """
    # create a VocabSorter for the DAG calculations
    vocabSorter = VocabSorter.VocabSorter()
    
    cmd = '''
    create temp table %s (
        _term_key int NOT NULL,
        sequencenum int NOT NULL,
        _annot_key int NOT NULL
    )
    ''' % C.VOCAB_DAG_SORT_TEMP_TABLE
    
    dbAgnostic.execute(cmd)
    
    cmd = '''
    select t._term_key,
    va._annot_key
    from voc_term t 
    join voc_annot va on
        va._term_key = t._term_key
    '''
    (cols, rows) = dbAgnostic.execute(cmd)
    termKeyCol = dbAgnostic.columnNumber (cols, '_term_key')
    annotKeyCol = dbAgnostic.columnNumber (cols, '_annot_key')
    
    # write to a temp file for BCP
    temp = tempfile.TemporaryFile()
    try:
    
        for row in rows:
            termKey = row[termKeyCol]
            annotKey = row[annotKeyCol]
            
            byDag = vocabSorter.sequenceNum(termKey)
            
            temp.write("%s\t%s\t%s\n" % (termKey, byDag, annotKey) )
        
        temp.seek(0)
    
        dbAgnostic.bcp(temp, C.VOCAB_DAG_SORT_TEMP_TABLE)
        
    finally:
        temp.close()
    
    cmd = '''create index tmp_vocab_dag_sort_idx on %s (_annot_key)''' \
        % C.VOCAB_DAG_SORT_TEMP_TABLE
    
    dbAgnostic.execute(cmd)
    
    
def _createDagStructureSequenceNums():
    """
    Create the sequence num data by dag structure
    
    Adds to TERM_DAG_SORT_TEMP_TABLE
    """
    # create a VocabSorter for the DAG calculations
    vocabSorter = VocabSorter.VocabSorterAlpha()
    
    cmd = '''
    create temp table %s (
        _term_key int NOT NULL,
        sequencenum int NOT NULL,
        _annot_key int NOT NULL
    )
    ''' % C.TERM_DAG_SORT_TEMP_TABLE
    
    dbAgnostic.execute(cmd)
    
    cmd = '''
    select t._term_key,
    va._annot_key
    from voc_term t 
    join voc_annot va on
        va._term_key = t._term_key
    '''
    (cols, rows) = dbAgnostic.execute(cmd)
    termKeyCol = dbAgnostic.columnNumber (cols, '_term_key')
    annotKeyCol = dbAgnostic.columnNumber (cols, '_annot_key')
    
    # write to a temp file for BCP
    temp = tempfile.TemporaryFile()
    try:
    
        for row in rows:
            termKey = row[termKeyCol]
            annotKey = row[annotKeyCol]
            
            byDag = vocabSorter.getVocabDagTermSequenceNum(termKey)
            
            temp.write("%s\t%s\t%s\n" % (termKey, byDag, annotKey) )
        
        temp.seek(0)
    
        dbAgnostic.bcp(temp, C.TERM_DAG_SORT_TEMP_TABLE)
        
    finally:
        temp.close()
    
    cmd = '''create index tmp_term_dag_sort_idx on %s (_annot_key)''' \
        % C.TERM_DAG_SORT_TEMP_TABLE
    
    dbAgnostic.execute(cmd)
    
    
def _createObjectDagSequenceNums():
    """
    Create the sequence num data for various object type annotations
        by (object, dag sort)
    Adds to OBJECT_DAG_SORT_TEMP_TABLE
    """
    
    cmd = '''
    create temp table %s (
        sequencenum int NOT NULL,
        _annot_key int NOT NULL
    )
    ''' % C.OBJECT_DAG_SORT_TEMP_TABLE
    
    dbAgnostic.execute(cmd)
    
    # use vocabSorterAlpha
    vocabSorter = VocabSorter.VocabSorterAlpha()
    
    # do markers
    _createMarkerObjectSequenceNums(vocabSorter)
    
    # do genotypes
    _createGenotypeObjectSequenceNums(vocabSorter)
    
    
    cmd = '''create index tmp_object_dag_sort_idx on %s (_annot_key)''' \
        % C.OBJECT_DAG_SORT_TEMP_TABLE
    
    dbAgnostic.execute(cmd)
    
    
def _createMarkerObjectSequenceNums(vocabSorter):
    """
    Create the sequence num data for marker annotations
        by (marker symbol, dag sort)
    Adds to OBJECT_DAG_SORT_TEMP_TABLE
    
    Assumes OBJECT_DAG_SORT_TEMP_TABLE is created
    """
    
    # do marker sorts
    
    cmd = '''
    select m._marker_key,
    m.symbol
    from mrk_marker m
    where exists (select 1 from 
        voc_annot va join
        voc_annottype vat on
            vat._annottype_key = va._annottype_key
        where va._object_key = m._marker_key
        and vat._mgitype_key = %d
    )
    ''' % C.MARKER_MGITYPE
    
    (cols, rows) = dbAgnostic.execute(cmd)
    markerKeyCol = dbAgnostic.columnNumber (cols, '_marker_key')
    symbolCol = dbAgnostic.columnNumber (cols, 'symbol')
    
    markers = []
    for row in rows:
        markers.append((row[markerKeyCol], row[symbolCol]))
    markers.sort(key=lambda x: x[1], cmp=symbolsort.nomenCompare)
    
    markerSeqMap = {}
    seqnum = 1
    for marker in markers:
        markerSeqMap[marker[0]] = seqnum
        seqnum += 1
        
    # add marker sorts to temp table
    
    cmd = '''
    select t._term_key,
    va._object_key as _marker_key,
    va._annot_key
    from voc_term t 
    join voc_annot va on
        va._term_key = t._term_key
    join voc_annottype vat on
        vat._annottype_key = va._annottype_key
    where vat._mgitype_key = %d
    ''' % C.MARKER_MGITYPE
    
    (cols, rows) = dbAgnostic.execute(cmd)
    termKeyCol = dbAgnostic.columnNumber (cols, '_term_key')
    markerKeyCol = dbAgnostic.columnNumber (cols, '_marker_key')
    annotKeyCol = dbAgnostic.columnNumber (cols, '_annot_key')
    
    # write to a temp file for BCP
    temp = tempfile.TemporaryFile()
    try:
    
        annotsToSort = []
        for row in rows:
            termKey = row[termKeyCol]
            markerKey = row[markerKeyCol]
            annotKey = row[annotKeyCol]
            
            byDag = vocabSorter.getVocabDagTermSequenceNum(termKey)
            byMarker = markerSeqMap[markerKey]
            
            annotsToSort.append((annotKey, (byMarker, byDag)))
            
        annotsToSort.sort(key=lambda x: x[1])
        
        seqnum = 1
        for annot in annotsToSort:
            
            temp.write("%s\t%s\n" % (seqnum, annot[0]) )
            seqnum += 1
            
        logger.debug("wrote %d marker/annotation seqnums" % seqnum)
        temp.seek(0)
    
        dbAgnostic.bcp(temp, C.OBJECT_DAG_SORT_TEMP_TABLE)
        
    finally:
        temp.close()
        
    
def _createGenotypeObjectSequenceNums(vocabSorter):
    """
    Create the sequence num data for marker annotations
        by (genotype/alleles, dag sort)
    Adds to OBJECT_DAG_SORT_TEMP_TABLE
    
    Assumes OBJECT_DAG_SORT_TEMP_TABLE is created
    """
        
    # 2) do genotype sorts
    
    # use AlleleAndGenotypeSorter
    alleleAndGenotypeSorter = AlleleAndGenotypeSorter
    
    # add genotype sorts to temp table
    
    cmd = '''
    select t._term_key,
    va._object_key as _genotype_key,
    va._annot_key
    from voc_term t 
    join voc_annot va on
        va._term_key = t._term_key
    join voc_annottype vat on
        vat._annottype_key = va._annottype_key
    where vat._mgitype_key = %d
    ''' % C.GENOTYPE_MGITYPE
    
    (cols, rows) = dbAgnostic.execute(cmd)
    termKeyCol = dbAgnostic.columnNumber (cols, '_term_key')
    genotypeKeyCol = dbAgnostic.columnNumber (cols, '_genotype_key')
    annotKeyCol = dbAgnostic.columnNumber (cols, '_annot_key')
        
    # write to a temp file for BCP
    temp = tempfile.TemporaryFile()
    try:
        
        annotsToSort = []
        for row in rows:
            termKey = row[termKeyCol]
            genotypeKey = row[genotypeKeyCol]
            annotKey = row[annotKeyCol]
            
            byDag = vocabSorter.getVocabDagTermSequenceNum(termKey)
            byGenotype = alleleAndGenotypeSorter.getGenotypeSequenceNum(genotypeKey)
            
            annotsToSort.append((annotKey, (byGenotype, byDag)))
            
        annotsToSort.sort(key=lambda x: x[1])
        
        seqnum = 1
        for annot in annotsToSort:
            
            temp.write("%s\t%s\n" % (seqnum, annot[0]) )
            seqnum += 1
        
        logger.debug("wrote %d genotype/annotation seqnums" % seqnum)
        temp.seek(0)
        
        dbAgnostic.bcp(temp, C.OBJECT_DAG_SORT_TEMP_TABLE)
        
    finally:
        temp.close()
        
        
    # TODO (kstone): refactor AlleleAndGenotypeSorter to 
    #    not use globals for better memory scoping
    alleleAndGenotypeSorter.GENOTYPE_SEQ_NUM = None
    alleleAndGenotypeSorter.ALLELE_SEQ_NUM = None
    
    
def _createIsoformSequenceNums():
    """
    Create the sequence num data by isoform (a GO annotation property)
    
    Adds to ISOFORM_SORT_TEMP_TABLE
    """
    
    cmd = '''
    create temp table %s (
        isoform text NOT NULL,
        sequencenum int NOT NULL,
        _annotevidence_key int NOT NULL,
        _annot_key int NOT NULL
    )
    ''' % C.ISOFORM_SORT_TEMP_TABLE
    
    dbAgnostic.execute(cmd)
    
    
    # query the correct keys to use for GO isoform properties
    isoformProcessor = go_isoforms.Processor()
    propertyTermKeys = isoformProcessor.querySanctionedPropertyTermKeys()

    propertyKeyClause = ",".join([str(k) for k in propertyTermKeys])
    
    
    cmd = '''
    select ve._annot_key,
        ve._annotevidence_key,
        vep.value
    from voc_evidence ve
    join voc_evidence_property vep on
        vep._annotevidence_key = ve._annotevidence_key
    where vep._propertyterm_key in (%s)
    ''' % ( propertyKeyClause )
    
    (cols, rows) = dbAgnostic.execute(cmd)
    annotKeyCol = dbAgnostic.columnNumber (cols, '_annot_key')
    evidenceKeyCol = dbAgnostic.columnNumber (cols, '_annotevidence_key')
    valueCol = dbAgnostic.columnNumber (cols, 'value')
    
    # calculate the isoform sort order
    isoforms = []
    for row in rows:
        isoformValues = isoformProcessor.processValue( row[valueCol] )
        for isoform in isoformValues:
            isoforms.append( isoform )
    isoforms.sort(cmp=symbolsort.nomenCompare)
    
    isoformSeqnum = {}
    seqnum = 1
    for isoform in isoforms:
        isoformSeqnum[isoform] = seqnum
        seqnum += 1
    
        
    # add isoform sorts to temp table
    
    # write to a temp file for BCP
    temp = tempfile.TemporaryFile()
    try:
        
        for row in rows:
            
            annotKey = row[annotKeyCol]
            evidenceKey = row[evidenceKeyCol]
            
            # find the lowest sequencenum for all the possible isoforms
            isoformValues = isoformProcessor.processValue( row[valueCol] )
            seqnum = 0
            for isoform in isoformValues:
                if seqnum > 0:
                    seqnum = min(isoformSeqnum[isoform], seqnum)
                else:
                    seqnum = isoformSeqnum[isoform]
            
            temp.write("%s\t%s\t%s\t%s\n" % (isoform, seqnum, evidenceKey, annotKey) )
            
        logger.debug("wrote %d isoform seqnums" % seqnum)
        temp.seek(0)
    
        dbAgnostic.bcp(temp, C.ISOFORM_SORT_TEMP_TABLE)
        
    finally:
        temp.close()
        
    
    cmd = '''create index tmp_isoform_sort_idx on %s (_annot_key)''' \
        % C.ISOFORM_SORT_TEMP_TABLE
    
    dbAgnostic.execute(cmd)
    
        

def queryByVocabSeqnums():
        """
        Query the vocab sequence nums
        
        Returns {vocab: sequencenum}
        
        Assumes VOCAB_SORT_TEMP_TABLE has been created
            during _createSequenceNumTables() initialization
        """
        
        cmd = '''select * from %s''' % C.VOCAB_SORT_TEMP_TABLE
        byVocabMap = {}
        
        (cols, rows) = dbAgnostic.execute(cmd)
        seqnumCol = dbAgnostic.columnNumber (cols, 'sequencenum')
        vocabCol = dbAgnostic.columnNumber (cols, 'vocab')
        
        for row in rows:
            byVocabMap[row[vocabCol]] = row[seqnumCol]
        
        return byVocabMap
        
        
def queryByAnnotTypeSeqnums():
    """
    Query the annottype sequence nums
    
    Returns {annottype: sequencenum}
    
    Assumes ANNOTTYPE_SORT_TEMP_TABLE has been created
        during _createSequenceNumTables() initialization
    """
    
    cmd = '''select * from %s''' % C.ANNOTTYPE_SORT_TEMP_TABLE
    byAnnotTypeMap = {}
    
    (cols, rows) = dbAgnostic.execute(cmd)
    
    # make query results editable
    rows = dbAgnostic.tuplesToLists(rows)
    
    # transform the annotation types
    transform.transformAnnotationType(cols, rows)
    
    seqnumCol = dbAgnostic.columnNumber (cols, 'sequencenum')
    annotTypeCol = dbAgnostic.columnNumber (cols, 'annottype')
    
    for row in rows:
        byAnnotTypeMap[row[annotTypeCol]] = row[seqnumCol]
    
    return byAnnotTypeMap


def queryByTermAlphaSeqnums(annotBatchTableName):
    """
    Query the term alpha sequence nums for
        _annot_key in annotBatchTableName
    
    Returns {term: sequencenum}
    
    Assumes TERM_SORT_TEMP_TABLE has been created
        during _createSequenceNumTables() initialization
    """
    
    cmd = '''
    select * 
    from %s tst
    join %s abt on
        abt._annot_key = tst._annot_key
    ''' % (C.TERM_SORT_TEMP_TABLE, annotBatchTableName)
    
    byTermAlphaMap = {}
    
    (cols, rows) = dbAgnostic.execute(cmd)
    seqnumCol = dbAgnostic.columnNumber (cols, 'sequencenum')
    termKeyCol = dbAgnostic.columnNumber (cols, '_term_key')
    
    for row in rows:
        byTermAlphaMap[row[termKeyCol]] = row[seqnumCol]
    
    return byTermAlphaMap


def queryByTermDagSeqnums(annotBatchTableName):
    """
    Query the term DAG sequence nums for
        _annot_key in annotBatchTableName
    
    Returns {term: sequencenum}
    
    Assumes TERM_DAG_SORT_TEMP_TABLE has been created
        during _createSequenceNumTables() initialization
    """
    
    cmd = '''
    select * 
    from %s dst
    join %s abt on
        abt._annot_key = dst._annot_key
    ''' % (C.TERM_DAG_SORT_TEMP_TABLE, annotBatchTableName)
    
    byTermDagMap = {}
    
    (cols, rows) = dbAgnostic.execute(cmd)
    seqnumCol = dbAgnostic.columnNumber (cols, 'sequencenum')
    termKeyCol = dbAgnostic.columnNumber (cols, '_term_key')
    
    for row in rows:
        byTermDagMap[row[termKeyCol]] = row[seqnumCol]
    
    return byTermDagMap


def queryByVocabDagSeqnums(annotBatchTableName):
    """
    Query the vocab DAG sequence nums for
        _annot_key in annotBatchTableName
    
    Returns {term: sequencenum}
    
    Assumes VOCAB_DAG_TEMP_TABLE has been created
        during _createSequenceNumTables() initialization
    """
    
    cmd = '''
    select * 
    from %s dst
    join %s abt on
        abt._annot_key = dst._annot_key
    ''' % (C.VOCAB_DAG_SORT_TEMP_TABLE, annotBatchTableName)
    
    byVocabDagMap = {}
    
    (cols, rows) = dbAgnostic.execute(cmd)
    seqnumCol = dbAgnostic.columnNumber (cols, 'sequencenum')
    termKeyCol = dbAgnostic.columnNumber (cols, '_term_key')
    
    for row in rows:
        byVocabDagMap[row[termKeyCol]] = row[seqnumCol]
    
    return byVocabDagMap


def queryByObjectDagSeqnums(annotBatchTableName):
    """
    Query the Object (marker/genotype/etc) DAG sequence nums for
        _annot_key in annotBatchTableName
    
    Returns {_annot_key: sequencenum}
    
    Assumes OBJECT_DAG_SORT_TEMP_TABLE has been created
        during _createSequenceNumTables() initialization
    """
    
    cmd = '''
    select * 
    from %s dst
    join %s abt on
        abt._annot_key = dst._annot_key
    ''' % (C.OBJECT_DAG_SORT_TEMP_TABLE, annotBatchTableName)
    
    byObjectDagMap = {}
    
    (cols, rows) = dbAgnostic.execute(cmd)
    seqnumCol = dbAgnostic.columnNumber (cols, 'sequencenum')
    annotKeyCol = dbAgnostic.columnNumber (cols, '_annot_key')
    
    for row in rows:
        byObjectDagMap[row[annotKeyCol]] = row[seqnumCol]
    
    return byObjectDagMap



def queryByIsoformSeqnums(annotBatchTableName):
    """
    Query the Isoform (a GO property) sequence nums for
        _annot_key in annotBatchTableName
    
    Returns {_annotevidence_key: sequencenum}
    
    Assumes ISOFORM_SORT_TEMP_TABLE has been created
        during _createSequenceNumTables() initialization
    """
    
    cmd = '''
    select * 
    from %s ist
    join %s abt on
        abt._annot_key = ist._annot_key
    ''' % (C.ISOFORM_SORT_TEMP_TABLE, annotBatchTableName)
    
    byIsoformMap = {}
    
    (cols, rows) = dbAgnostic.execute(cmd)
    seqnumCol = dbAgnostic.columnNumber (cols, 'sequencenum')
    evidenceKeyCol = dbAgnostic.columnNumber (cols, '_annotevidence_key')
    
    for row in rows:
        byIsoformMap[row[evidenceKeyCol]] = row[seqnumCol]
    
    return byIsoformMap

