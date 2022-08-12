#i Name: GXDUniUtils.py
# Purpose: Contains code for bringing classical expression results and RNA-Seq
#       consolidated sample measurements together into a set of universal
#       expression results.  These eventually feed into the gxdResult Solr index
#       that feeds the expression summary pages in the fewi.

import dbAgnostic
import logger
import symbolsort
import VocabSorter

###--- Global Variables ---###

# List of dictionaries, each of which describes an RNA-Seq experiment ID with at least
# these fields:  _Experiment_key, accID
EXPT_ID_LIST = None

# This temporary table maps from a classical result key or an RNA-Seq
# consolidated measurement key to its corresponding universal key (uni_key)
# and contains values needed for sorting the results in various ways.
TMP_KEYSTONE = 'tmp_keystone'

# This temporary table maps from a current mouse marker key to a precomputed
# integer sequence number (sorted by symbol).
TMP_MARKER_SEQNUM = 'tmp_marker_seqnum'

# This temporary table maps from a term key for (EMAPA) anatomical structures
# to a precomputed integer sequence number (sorted by DFS).
TMP_STRUCTURE_SEQNUM = 'tmp_structure_seqnum'

# This temporary table maps from an assay type key to a precomputed sequence
# integer sequence number (with RNA-Seq last).
TMP_ASSAYTYPE_SEQNUM = 'tmp_assaytype_seqnum'

# This temporary table maps from a Gel lane key to a precomputed flag that
# indicates whether expression was detected (1) or not (0).
TMP_GELLANE_DETECTED = 'tmp_gellane_detected'

# This temporary table maps from an in situ result key to a precomputed flag
# that indicates whether expression was detected (1) or not (0).
TMP_ISRESULT_DETECTED = 'tmp_isresult_detected'

# This temporary table maps from a reference key to a precomputed sequence
# integer sequence number (sorted by J: number).
TMP_REFERENCE = 'tmp_reference'

# This temporary table maps from an RNA-Seq experiment key to a precomputed sequence
# integer sequence number (sorted by experiment ID number).
TMP_RNASEQ_ID = 'tmp_rnaseq_id'

# Dictionary containing as keys the names of temp tables already produced.
EXISTING = {}

# Indexes on temporary tables are numbered with an ascending integer so they
# will be uniquely named.
INDEX_COUNT = 0

# Name of table where we assign unique keys to classical results.
TMP_CLASSICAL_KEYS = 'tmp_classical_keys'

###--- Public Functions ---###

def getClassicalKeyTable():
        # build (if necessary) and get a table that identifies the assigned key value for each
        # classical expression result
        
        if not exists(TMP_CLASSICAL_KEYS):
                _assignClassicalKeys()
                
        return TMP_CLASSICAL_KEYS

def setExptIDList(idList):
        # set the given idList for use in sorting by RNA-Seq experiment IDs as reference IDs.

        global EXPT_ID_LIST
        EXPT_ID_LIST = idList
        return

def getSortedTable(tableName, sortString):
        # Create a sorted table with the given 'tableName', with columns
        # equivalent to the TMP_KEYSTONE table -- sorted according to the
        # ORDER BY string specified in 'sortString' -- and with sequence
        # assigned in a new column (seqNum).

        if exists(tableName):
                return tableName

        return _buildSortedTable(tableName, sortString)

def exists(name):
        # determine whether temp table 'name' exists (True) or not (False)
        return (name in EXISTING)

def setExists(name):
        # note that temp table 'name' now exists
        EXISTING[name] = True

def getKeystoneTable():
        # get the name of the keystone table, creating it if needed

        if not exists(TMP_KEYSTONE):
                _buildKeystoneTable()
        return TMP_KEYSTONE

def getMarkerSeqnumTable():
        # get the name of the marker sequence number table, creating if needed

        if not exists(TMP_MARKER_SEQNUM):
                _buildMarkerSeqnumTable()
        return TMP_MARKER_SEQNUM

def getAssayTypeSeqnumTable():
        # get the name of the assay type sequence number table, creating it if
        # needed

        if not exists(TMP_ASSAYTYPE_SEQNUM):
                _buildAssayTypeSeqnumTable()
        return TMP_ASSAYTYPE_SEQNUM

def getInSituResultTable():
        if not exists(TMP_ISRESULT_DETECTED):
                _getInSituResultTable()
        return TMP_ISRESULT_DETECTED

def getGelLaneTable():
        if not exists(TMP_GELLANE_DETECTED):
                _getGelLaneTable()
        return TMP_GELLANE_DETECTED

def getStructureSeqnumTable():
        # get the name of the (EMAPA) structure sequence number table, creating
        # it if needed

        if not exists(TMP_STRUCTURE_SEQNUM):
                _buildStructureSeqnumTable()
        return TMP_STRUCTURE_SEQNUM

def createIndex(table, field, isUnique = False):
        # create a single-field index on the given table
        global INDEX_COUNT

        unique = ''
        if isUnique:
                unique = 'unique '

        INDEX_COUNT = INDEX_COUNT + 1
        name = 'tmpidx%d' % INDEX_COUNT

        cmd = 'create %sindex %s on %s (%s)' % (unique, name, table, field)
        dbAgnostic.execute(cmd)
        logger.info('Created index on %s(%s)' % (table, field))
        return

###--- Private Functions ---###

def _getRowCount(name, whereClause = ''):
        # get the count of rows for the given table, including an optional
        # WHERE clause (if specified)
        cmd = 'select count(1) from %s %s' % (name, whereClause)
        cols, rows = dbAgnostic.execute(cmd)
        return rows[0][0]

def _getMaxValue(table, field, whereClause = ''):
        # get the count of rows for the given table, including an optional
        # WHERE clause (if specified)
        cmd = 'select max(%s) from %s %s' % (field, table, whereClause)
        cols, rows = dbAgnostic.execute(cmd)
        return rows[0][0]

def _assignClassicalKeys():
        # Build the temp table where we assign unique keys to classical expression results.
        # Assumes we've already checked that the table does not exist yet.
        
        logger.info('Began building: %s' % TMP_CLASSICAL_KEYS)

        # Temp table will have the five keys that uniquely define a result, plus an assigned key that can
        # be used to uniquely identify that result.
        cmd0 = '''create temporary table %s (
                _Assay_key int not null,
                _Result_key int not null,
                _Stage_key int not null,
                _Emapa_Term_key int not null,
                _CellType_Term_key int null,
                _Assigned_key int not null)''' % TMP_CLASSICAL_KEYS
        dbAgnostic.execute(cmd0)
        
        # First add in situ results to the temp table, assigning new key based on a prescribed ordering.
        # Remember to exclude results for recombinase data (require isForGXD = 1).
        cmd1 = '''insert into %s
                select s._Assay_key, r._Result_key, i._Stage_key, i._Emapa_Term_key, c._CellType_Term_key,
                        row_number() over (order by s._Assay_key, r._Result_key, i._Stage_key, i._Emapa_Term_key,
                            c._CellType_Term_key)
                from gxd_specimen s
                inner join gxd_insituresult r on (s._Specimen_key = r._Specimen_key)
                inner join gxd_isresultstructure i on (r._Result_key = i._Result_key)
                left outer join gxd_isresultcelltype c on (r._Result_key = c._Result_key)
                where exists (select 1 from gxd_expression ge where s._Assay_key = ge._Assay_key and ge.isForGXD = 1)
                order by s._Assay_key, r._Result_key, i._Stage_key, i._Emapa_Term_key''' % TMP_CLASSICAL_KEYS
        dbAgnostic.execute(cmd1)
        
        maxKey = _getMaxValue(TMP_CLASSICAL_KEYS, '_Assigned_key')
        logger.info(' - added %d in situ results' % maxKey)

        # Then add blot results to the temp table, likewise assigning them new keys based on the prescribed
        # ordering.  Note that these are numbered after the in situ assays and that the gel lane key serves
        # as the result key for these.
        cmd3 = '''insert into %s
                select g._Assay_key, g._GelLane_key, gs._Stage_key, gs._Emapa_Term_key, null as _CellType_Term_key,
                        %d + row_number() over (order by g._Assay_key, g._GelLane_key, gs._Stage_key, gs._Emapa_Term_key)
                from gxd_gellane g,
                        gxd_gellanestructure gs,
                        voc_term_emaps vte,
                        voc_term vtc
                where g._GelControl_key = vtc._term_key and vtc.term = 'No'
                        and g._GelLane_key = gs._GelLane_key
                        and vte._emapa_term_key = gs._emapa_term_key
                        and vte._stage_key = gs._stage_key
                order by g._Assay_key, g._GelLane_key, gs._Stage_key, gs._Emapa_Term_key''' % (TMP_CLASSICAL_KEYS, maxKey)
        dbAgnostic.execute(cmd3)
        
        rowCount = _getRowCount(TMP_CLASSICAL_KEYS)
        logger.info(' - added %d blot results' % (rowCount - maxKey))

        createIndex(TMP_CLASSICAL_KEYS, '_Assay_key')
        createIndex(TMP_CLASSICAL_KEYS, '_Result_key')
        createIndex(TMP_CLASSICAL_KEYS, '_Stage_key')
        createIndex(TMP_CLASSICAL_KEYS, '_Emapa_Term_key')
        createIndex(TMP_CLASSICAL_KEYS, '_CellType_Term_key')
        logger.info(' - added indexes')

        logger.info('Done building: %s' % TMP_CLASSICAL_KEYS)
        setExists(TMP_CLASSICAL_KEYS)
        return TMP_CLASSICAL_KEYS
        
def _getInSituResultTable():
        # Build the cache table of "is detected" values for in situ results,
        # returning the table's name once it's been built.  The table maps from
        # a _Result_key to a 0 (ambiguous, not specified), 1 (no), or 2 (yes) flag
        # that can be sorted by priority in descending order.

        if exists(TMP_ISRESULT_DETECTED):
                return TMP_ISRESULT_DETECTED

        logger.info('Began building: %s' % TMP_ISRESULT_DETECTED)

        # Need to build a temp table with all in situ result keys and a flag to
        # indicate whether there was expression detected in each.  Start each
        # flag as "no" and upgrade to "yes" as needed.

        cmd0 = '''create temporary table %s (
                _Result_key     int     not null,
                is_detected     int     not null
                )''' % TMP_ISRESULT_DETECTED
        dbAgnostic.execute(cmd0)
        logger.info("Created table");

        cmd1 = '''insert into %s
                select g._Result_key, case
                        when s.term in ('Ambiguous', 'Not Specified') then 0
                        when s.term in ('Absent', 'Not Applicable') then 1
                        else 2
                        end as is_detected
                from gxd_insituresult g, voc_term s
                where g._Strength_key = s._Term_key''' % TMP_ISRESULT_DETECTED
        
        dbAgnostic.execute(cmd1)
        createIndex(TMP_ISRESULT_DETECTED, '_Result_key')
        logger.info("Filled and indexed table")

        setExists(TMP_ISRESULT_DETECTED)
        logger.info('Done building: %s' % TMP_ISRESULT_DETECTED)
        return TMP_ISRESULT_DETECTED

def _getReferenceTable():
        # Build and get a cache table of reference keys and precomputed sequence
        # numbers for ordering them by author.  Return the table's name once
        # it's built.
        
        if exists(TMP_REFERENCE):
                return TMP_REFERENCE
        
        cmd1 = 'drop table if exists %s' % TMP_REFERENCE
        dbAgnostic.execute(cmd1)

        logger.info('Began building: %s' % TMP_REFERENCE)
        
        cmd0 = '''create temporary table %s (
                _Refs_key       int     not null,
                seqNum          int     not null
                )''' % TMP_REFERENCE
        dbAgnostic.execute(cmd0)

        # Note the fallback here to ordering by title if a reference has a null authors field; currently
        # there are no such papers cited in GXD data, but we have the fallback just in case.
        cmd1 = '''select _Refs_key, case
                        when authors is not null then authors
                        else title
                        end as authors
                from bib_refs r
                where exists (select 1 from gxd_expression e where e._Refs_key = r._Refs_key)'''

        cols, rows = dbAgnostic.execute(cmd1)
        keyCol = dbAgnostic.columnNumber(cols, '_Refs_key')
        authorsCol = dbAgnostic.columnNumber(cols, 'authors')

        toSort = []
        for row in rows:
                toSort.append( (row[keyCol], row[authorsCol]) )
        logger.info('Got %d references' % len(toSort))

        toSort.sort(key=_symbolCompare)
        logger.info('Sorted %d references by authors' % len(toSort))

        toLoad = []
        seqNum = 0
        for (key, authors) in toSort:
                seqNum = seqNum + 1
                toLoad.append( (key, seqNum) )

        dbAgnostic.batchInsert(TMP_REFERENCE, toLoad)
        
        createIndex(TMP_REFERENCE, '_Refs_key')
        logger.info('Filled and indexed table with %d rows' % _getRowCount(TMP_REFERENCE))
        
        logger.info('Done building: %s' % TMP_REFERENCE) 
        setExists(TMP_REFERENCE)
        return TMP_REFERENCE

def _getRnaSeqExptTable():
        # Build and get a cache table of RNA-Seq experiments and their sequence numbers
        # (for sorting by the reference column, which uses experiment IDs for RNA-Seq).
        
        if exists(TMP_RNASEQ_ID):
                return TMP_RNASEQ_ID
        
        if EXPT_ID_LIST == None:
                raise Exception('EXPT_ID_LIST is not defined.  Must call setExptIDList().')

        # will need other references to see what the max sequence number already is (so we
        # can put RNA-Seq experiments after it)
        referenceTable = _getReferenceTable()

        toSort = []
        for id in EXPT_ID_LIST:
                toSort.append ( (id['_experiment_key'], id['accid']) )
        toSort.sort(key=_symbolCompare)
        logger.info('Sorted experiment IDs')

        toLoad = []
        seqNum = _getMaxValue(referenceTable, 'seqNum')
        for (key, accID) in toSort:
                seqNum = seqNum + 1
                toLoad.append( (key, seqNum) )
        logger.info('Built ID list to batch load')

        logger.info('Began building: %s' % TMP_RNASEQ_ID)
        
        cmd0 = '''create temporary table %s (
                _Experiment_key int     not null,
                seqNum                  int     not null
                )''' % TMP_RNASEQ_ID
        dbAgnostic.execute(cmd0)
        
        dbAgnostic.batchInsert(TMP_RNASEQ_ID, toLoad)

        createIndex(TMP_RNASEQ_ID, '_Experiment_key')
        logger.info('Filled and indexed table with %d rows' % _getRowCount(TMP_RNASEQ_ID))

        logger.info('Done building: %s' % TMP_RNASEQ_ID)
        setExists(TMP_RNASEQ_ID)
        return TMP_RNASEQ_ID
        
def _getGelLaneTable():
        # Build the cache table of "is detected" values for in gel lanes,
        # returning the table's name once it's been built.  The table maps from
        # a _GelLane_key to a 0 (ambiguous, not specified), 1 (no), or 2 (yes) flag
        # that can be sorted by priority in descending order.

        if exists(TMP_GELLANE_DETECTED):
                return TMP_GELLANE_DETECTED

        logger.info('Began building: %s' % TMP_GELLANE_DETECTED)

        # Need to build a temp table with all gel lane keys and a flag to
        # indicate whether there was expression detected in any of the bands
        # for that lane.  Start each flag as "ambiguous / not specified" (0)
        # and upgrade to "no" (1) or "yes" (2) as needed.

        cmd0 = '''create temporary table %s (
                _GelLane_key    int     not null,
                is_detected     int     not null
                )''' % TMP_GELLANE_DETECTED
        dbAgnostic.execute(cmd0)
        logger.info("Created table");

        # start as ambiguous / not specified (0)
        cmd1 = '''insert into %s
                select gl._GelLane_key, 0 as is_detected
                from gxd_gellane gl''' % TMP_GELLANE_DETECTED
                
        dbAgnostic.execute(cmd1)
        createIndex(TMP_GELLANE_DETECTED, '_GelLane_key')
        logger.info("Filled and indexed table")

        # upgrade those having absent or not applicable (1)
        cmd2 = '''update %s
                set is_detected = 1
                where _GelLane_key in (select f._GelLane_key
                        from %s f, gxd_gelband gb, voc_term gs
                        where f._GelLane_key = gb._GelLane_key
                        and gb._Strength_key = gs._Term_key
                        and gs.term in ('Absent', 'Not Applicable')
                        )''' % (TMP_GELLANE_DETECTED, TMP_GELLANE_DETECTED)

        dbAgnostic.execute(cmd2)
        logger.info("Flagged negative expression")
        
        # further upgrade those having any other status (all positive expression) (2)
        cmd3 = '''update %s
                set is_detected = 2
                where _GelLane_key in (select f._GelLane_key
                        from %s f, gxd_gelband gb, voc_term gs
                        where f._GelLane_key = gb._GelLane_key
                        and gb._Strength_key = gs._Term_key
                        and gs.term not in ('Absent', 'Not Applicable', 'Ambiguous', 'Not Specified')
                        )''' % (TMP_GELLANE_DETECTED, TMP_GELLANE_DETECTED)
        
        dbAgnostic.execute(cmd3)
        logger.info("Flagged positive expression")
        
        setExists(TMP_GELLANE_DETECTED)
        logger.info('Done building: %s' % TMP_GELLANE_DETECTED)
        return TMP_GELLANE_DETECTED

def _buildKeystoneTable():
        # Build the TMP_KEYSTONE temp table.
        #
        # It is vitally important that the result_keys in the keystone table
        # match their corresponding tables for classical and RNA-Seq data.
        #
        # For classical data, result_keys in expression_result_summary are
        # assigned in the corresponding gatherer.  In that gatherer, these keys
        # are assigned (in the code) to results based on a prescribed order. 
        # This order is defined as:
        #       1. assay key
        #       2. result key (in situ assays) or gel lane key (gel assays)
        #       3. structure term key
        #
        # For RNA-Seq data, consolidated_measurement_keys in 
        # expression_ht_consolidated_sample_measurement are taken directly from
        # _RNASeqCombined_key in the production-style database's 
        # GXD_HTSample_RNASeqCombined table.

        classicalKeyTable = getClassicalKeyTable()
        assayTypeTable = getAssayTypeSeqnumTable()
        markerTable = getMarkerSeqnumTable()
        structureTable = getStructureSeqnumTable()
        gelLaneTable = _getGelLaneTable()
        isResultTable = _getInSituResultTable()
        referenceTable = _getReferenceTable()

        logger.info('Began building: %s' % TMP_KEYSTONE)

        cmd0 = '''create temporary table %s (
                uni_key                 int     not null,
                is_classical    int     not null,
                assay_type_key  int     not null,
                assay_key               int     not null,
                old_result_key  int     not null,
                result_key              int     not null,
                ageMin                  float   null,
                ageMax                  float   null,
                _Stage_key              int     not null,
                _Term_key               int     not null,
                by_structure    int     not null,
                by_marker               int     not null,
                by_assay_type   int     not null,
                is_detected             int     not null,
                by_reference    int     not null,
                _Genotype_key   int     not null,
                _CellType_Term_key    int    null
                )''' % TMP_KEYSTONE

        dbAgnostic.execute(cmd0)
        logger.info('Created temp table: %s' % TMP_KEYSTONE)

        cmd1 = '''with results as (
                select 1 as is_classical, a._AssayType_key, a._Assay_key, r._Result_key,
                        s.ageMin, s.ageMax, rs._stage_key, rs._Emapa_Term_key as _Term_key,
                        a._Marker_key, isr.is_detected, a._Refs_key, s._Genotype_key,
                        c._CellType_Term_key
                from gxd_specimen s
                inner join gxd_assay a on (s._Assay_key = a._Assay_key)
                inner join gxd_insituresult r on (s._Specimen_key = r._Specimen_key)
                inner join gxd_isresultstructure rs on (r._Result_key = rs._Result_key)
                inner join %s isr on (r._Result_key = isr._Result_key)
                left outer join gxd_isresultcelltype c on (r._Result_key = c._Result_key)
                where exists (select 1 from gxd_expression e where s._Assay_key = e._Assay_key and e.isForGxd = 1)
                union
                select 1 as is_classical, a._AssayType_key, g._Assay_key, g._GelLane_key as _Result_key,
                        g.ageMin, g.ageMax, s._stage_key, s._Emapa_Term_key as _Term_key,
                        a._Marker_key, glk.is_detected, a._Refs_key, g._Genotype_key, null as _CellType_Term_key
                from gxd_gellane g,
                        gxd_assay a,
                        gxd_gellanestructure s,
                        voc_term vtc,
                        %s glk
                where g._GelControl_key = vtc._term_key and vtc.term = 'No'
                        and g._Assay_key = a._Assay_key
                        and g._GelLane_key = s._GelLane_key
                        and exists (select 1 from gxd_expression e where g._GelLane_key = e._GelLane_key and e.isForGXD = 1)
                        and g._GelLane_key = glk._GelLane_key
                )
                insert into %s
                select distinct ck._Assigned_key, r.is_classical, r._AssayType_key, r._Assay_key, r._Result_key,
                        ck._Assigned_key, r.ageMin, r.ageMax, r._stage_key, r._Term_key, emapa.seqNum as emapaSeqNum,
                        mrk.seqNum as mrkSeqNum, aa.seqNum as atSeqNum, r.is_detected, ref.seqNum, r._Genotype_key,
                        r._CellType_Term_key
                from results r,
                        %s ck,
                        %s ref,
                        %s emapa,
                        %s mrk,
                        %s aa
                where r._Refs_key = ref._Refs_key
                        and r._Term_key = emapa._Term_key
                        and r._Marker_key = mrk._Marker_key
                        and r._AssayType_key = aa._AssayType_key
                        and r._Assay_key = ck._Assay_key
                        and r._Result_key = ck._Result_key
                        and r._Stage_key = ck._Stage_key
                        and r._Term_key = ck._Emapa_Term_key
                        and ((r._CellType_Term_key = ck._CellType_Term_key) or 
                            ((r._CellType_Term_key is null) and (ck._CellType_Term_key is null))
                            )
                order by ck._Assigned_key''' % (
                        isResultTable, gelLaneTable, TMP_KEYSTONE, TMP_CLASSICAL_KEYS,
                        referenceTable, structureTable, markerTable, assayTypeTable)

        dbAgnostic.execute(cmd1)
        classicalRows = _getRowCount(TMP_KEYSTONE)
        logger.info('Added %d classical rows' % classicalRows)

        cmd1b = '''select count(1) from %s k, gxd_assay a
                where k.assay_key = a._Assay_key
                        and a._AssayType_key in (%s)'''
        cols, rows = dbAgnostic.execute(cmd1b % (TMP_KEYSTONE, '1, 6, 9'))
        logger.info(' - %d In situ rows' % rows[0][0])

        cols, rows = dbAgnostic.execute(cmd1b % (TMP_KEYSTONE, '2, 3, 4, 5, 8'))
        logger.info(' - %d Gel rows' % rows[0][0])

        # Get to the set of unique sample data for each consolidated sample.
        cmd2 = '''select distinct rsc._RNASeqCombined_key, s._Emapa_key,
                        s._Stage_key,
                        mrk.seqNum as mrkSeqNum,
                        s.ageMin, s.ageMax,
                        case 
                        when level.term = 'Below Cutoff' then 1
                        else 2
                        end as is_detected,
                        s._Experiment_key,
                        s._Genotype_key
                into temporary table rscmap
                from gxd_htsample_rnaseq rsc, gxd_htsample s, %s mrk,
                        gxd_htsample_rnaseqcombined c, voc_term level
                where rsc._Sample_key = s._Sample_key
                        and rsc._RNASeqCombined_key = c._RNASeqCombined_key
                        and c._Level_key = level._Term_key
                        and rsc._Marker_key = mrk._Marker_key'''

        dbAgnostic.execute(cmd2 % markerTable)
        logger.info('Added %d rows to rscmap table' % _getRowCount('rscmap'))
        createIndex('rscmap', '_Emapa_key')
        
        cmd3 = 'select max(_AssayType_key) from %s' % assayTypeTable
        cols, rows = dbAgnostic.execute(cmd3)
        rnaseq = rows[0][0]

        cmd4 = '''select seqNum
                from %s
                where _AssayType_key = %s''' %(assayTypeTable, rnaseq)
        cols, rows = dbAgnostic.execute(cmd4)
        rnaSeqType = rows[0][0]

        # get the ordering table for experiment IDs
        exptOrderingTable = _getRnaSeqExptTable()

        # Insert RNA-Seq rows into the keystone table
        cmd5 = '''insert into %s
                select distinct %d + rm._RNASeqCombined_key, 0, 99,
                        rm._RNASeqCombined_key,
                        rm._RNASeqCombined_key,
                        rm._RNASeqCombined_key, rm.ageMin, rm.ageMax,
                        rm._Stage_key, rm._Emapa_key, emapa.seqNum,
                        rm.mrkSeqNum, %d, rm.is_detected, expt.seqNum, rm._Genotype_key
                from rscmap rm,
                        %s emapa,
                        %s expt
                where rm._Emapa_key = emapa._Term_key
                        and rm._Experiment_key = expt._Experiment_key
                order by rm._RNASeqCombined_key, rm.mrkSeqNum''' % (
                        TMP_KEYSTONE, classicalRows, rnaSeqType, structureTable,
                        exptOrderingTable)
        dbAgnostic.execute(cmd5)

        rnaseqRows = _getRowCount(TMP_KEYSTONE) - classicalRows
        logger.info('Added %d RNA-Seq rows' % rnaseqRows) 

        setExists(TMP_KEYSTONE)
        logger.info('Done building: %s' % TMP_KEYSTONE)
        return

def _symbolCompare(a):
        # get a key for sorting by (symbol, marker key)
        return (symbolsort.splitter(a[1]), a[0])

def _buildMarkerSeqnumTable():
        # build the marker sequence number table

        logger.info('Began building: %s' % TMP_MARKER_SEQNUM)

        cmd0 = '''select _Marker_key, symbol
                from mrk_marker
                where _Marker_Status_key = 1 and _Organism_key = 1'''

        cols, rows = dbAgnostic.execute(cmd0)
        keyCol = dbAgnostic.columnNumber(cols, '_Marker_key')
        symbolCol = dbAgnostic.columnNumber( cols, 'symbol')

        toSort = []
        for row in rows:
                toSort.append( (row[keyCol], row[symbolCol]) )
        logger.info('Got %d marker symbols' % len(toSort))

        toSort.sort(key=_symbolCompare)
        logger.info('Sorted %d marker symbols' % len(toSort))

        toLoad = []
        seqNum = 0
        for (key, symbol) in toSort:
                seqNum = seqNum + 1
                toLoad.append( (key, seqNum) )

        cmd1 = '''create temporary table %s (
                _Marker_key     int     not null,
                seqNum          int     not null
                )'''
        dbAgnostic.execute(cmd1 % TMP_MARKER_SEQNUM)
        dbAgnostic.batchInsert(TMP_MARKER_SEQNUM, toLoad)
        logger.info('Loaded table with %d rows' % _getRowCount(TMP_MARKER_SEQNUM))

        createIndex(TMP_MARKER_SEQNUM, '_Marker_key', True)
        createIndex(TMP_MARKER_SEQNUM, 'seqNum', True)

        setExists(TMP_MARKER_SEQNUM)
        logger.info('Done building: %s' % TMP_MARKER_SEQNUM)
        return

def _buildAssayTypeSeqnumTable():
        # build the assay type sequence number table

        logger.info('Began building: %s' % TMP_ASSAYTYPE_SEQNUM)

        cmd0 = '''select _AssayType_key, sequenceNum
                from gxd_assaytype
                order by sequenceNum'''

        cols, rows = dbAgnostic.execute(cmd0)
        keyCol = dbAgnostic.columnNumber(cols, '_AssayType_key')

        # The above query returns pre-ordered classical assay types.
        toLoad = []
        seqNum = 0
        for row in rows:
                seqNum = seqNum + 1
                toLoad.append( (row[keyCol], seqNum) )

        # Plus add a row for RNA-Seq.
        toLoad.append( (99, seqNum + 1) )
        logger.info('Got and sorted %d assay types' % len(toLoad))

        cmd1 = '''create temporary table %s (
                _AssayType_key  int     not null,
                seqNum          int     not null
                )'''
        dbAgnostic.execute(cmd1 % TMP_ASSAYTYPE_SEQNUM)
        dbAgnostic.batchInsert(TMP_ASSAYTYPE_SEQNUM, toLoad)
        logger.info('Loaded table with %d rows' % _getRowCount(TMP_ASSAYTYPE_SEQNUM))

        createIndex(TMP_ASSAYTYPE_SEQNUM, '_AssayType_key', True)

        setExists(TMP_ASSAYTYPE_SEQNUM)
        logger.info('Done building: %s' % TMP_ASSAYTYPE_SEQNUM)
        return

def _termCompare(a):
        # get a key for sorting by (sequence num, term key)
        return (a[1], a[0])

def _buildStructureSeqnumTable():
        # build the (EMAPA) structure sequence number table

        logger.info('Began building: %s' % TMP_STRUCTURE_SEQNUM)

        VocabSorter.setVocabs(90)

        # get EMAPA term keys used in either high-throughput experiments or
        # classical experiments
        cmd0 = '''select t._Term_key
                from voc_term t
                where t._Vocab_key = 90'''

        cols, rows = dbAgnostic.execute(cmd0)
        keyCol = dbAgnostic.columnNumber(cols, '_Term_key')

        toSort = []
        for row in rows:
                key = row[keyCol]
                toSort.append( (key, VocabSorter.getSequenceNum(key) ) )
        logger.info('Got %d terms' % len(toSort))

        toSort.sort(key=_termCompare)
        logger.info('Sorted %d terms' % len(toSort))

        cmd1 = '''create temporary table %s (
                _Term_key       int     not null,
                seqNum          int     not null
                )'''
        dbAgnostic.execute(cmd1 % TMP_STRUCTURE_SEQNUM)
        dbAgnostic.batchInsert(TMP_STRUCTURE_SEQNUM, toSort)
        logger.info('Loaded table with %d rows' % _getRowCount(TMP_STRUCTURE_SEQNUM))

        createIndex(TMP_STRUCTURE_SEQNUM, '_Term_key', True)

        setExists(TMP_STRUCTURE_SEQNUM)
        logger.info('Done building: %s' % TMP_STRUCTURE_SEQNUM)
        return

def _buildSortedTable(tableName, sortString):
        # returns the tableName once it has been created and populated

        keystone = getKeystoneTable()

        logger.info('Began building: %s' % tableName)

        cmd0 = '''create temporary table %s (
                uni_key                 int     not null,
                is_classical    int     not null,
                result_key              int     not null,
                ageMin                  float   null,
                ageMax                  float   null,
                _Stage_key              int     not null,
                by_structure    int     not null,
                by_marker               int     not null,
                by_assay_type   int     not null,
                is_detected             int     not null,
                seqNum                  int     not null
                )''' % tableName

        dbAgnostic.execute(cmd0)
        logger.info('Created temp table: %s' % tableName)

        cmd1 = '''insert into %s
                select uni_key, is_classical, result_key, ageMin, ageMax,
                        _Stage_key, by_structure, by_marker, by_assay_type,
                        is_detected, row_number() over (order by %s)
                from %s
                order by %s''' % (tableName, sortString, keystone, sortString)

        dbAgnostic.execute(cmd1)
        logger.info('Populated: %s' % tableName)

        setExists(tableName)
        logger.info('Done building: %s' % tableName)
        dbAgnostic.commit()
        return tableName
