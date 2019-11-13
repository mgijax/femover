# Name: GXDUniUtils.py
# Purpose: Contains code for bringing classical expression results and RNA-Seq
#	consolidated sample measurements together into a set of universal
#	expression results.  These eventually feed into the gxdResult Solr index
#	that feeds the expression summary pages in the fewi.

import dbAgnostic
import logger
import symbolsort
import VocabSorter

###--- Global Variables ---###

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

# This temporary table maps from current mouse marker keys to sequence numbers
# when the markers are sorted by symbol.
TMP_BY_SYMBOL = 'tmp_by_symbol'

# This temporary table maps from current mouse marker keys and assay type keys
# to sequence numbers when the markers are sorted by symbol primarily and
# assay types secondarily.
TMP_BY_SYMBOL_ASSAY_TYPE = 'tmp_by_symbol_assay_type'

# This temporary table maps from sample keys to sequence numbers when they are
# sorted by age, structure, and stage.
TMP_BY_AGE_STRUCTURE_STAGE = 'tmp_by_age_structure_stage'

# This temporary table maps from sample keys to sequence numbers when they are
# sorted by structure, stage, and age.
TMP_BY_STRUCTURE_STAGE_AGE = 'tmp_by_structure_stage_age'

# Dictionary containing as keys the names of temp tables already produced.
EXISTING = {}

# Indexes on temporary tables are numbered with an ascending integer so they
# will be uniquely named.
INDEX_COUNT = 0

###--- Public Functions ---###

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

def _getInSituResultTable():
	# Build the cache table of "is expressed" values for in situ results,
	# returning the table's name once it's been built.

	if exists(TMP_ISRESULT_DETECTED):
		return TMP_ISRESULT_DETECTED

	logger.info('Began building: %s' % TMP_ISRESULT_DETECTED)

	# Need to build a temp table with all in situ result keys and a flag to
	# indicate whether there was expression detected in each.  Start each
	# flag as "no" and upgrade to "yes" as needed.

	cmd0 = '''create temporary table %s (
		_Result_key	int	not null,
		is_detected	int	not null
		)''' % TMP_ISRESULT_DETECTED
	dbAgnostic.execute(cmd0)
	logger.info("Created table");

	cmd1 = '''with positiveCounts as (
		select gi._Result_key,
			count(distinct gs._Strength_key) as positiveCount
		from gxd_insituresult gi
		left outer join gxd_strength gs on (
			gi._Strength_key = gs._Strength_key
			and gs.strength not in ('Absent', 'Not Applicable') )
		group by 1)
		insert into %s
		select _Result_key, positiveCount
		from positiveCounts''' % TMP_ISRESULT_DETECTED

	dbAgnostic.execute(cmd1)
	createIndex(TMP_ISRESULT_DETECTED, '_Result_key')
	logger.info("Filled and indexed table")

	setExists(TMP_ISRESULT_DETECTED)
	logger.info('Done building: %s' % TMP_ISRESULT_DETECTED)
	return TMP_ISRESULT_DETECTED

def _getGelLaneTable():
	# Build the cache table of "is expressed" values for gel lanes,
	# returning the table's name once it's been built.

	if exists(TMP_GELLANE_DETECTED):
		return TMP_GELLANE_DETECTED

	logger.info('Began building: %s' % TMP_GELLANE_DETECTED)

	# Need to build a temp table with all gel lane keys and a flag to
	# indicate whether there was expression detected in any of the bands
	# for that lane.  Start each flag as "no" and upgrade to "yes" as
	# needed.

	cmd0 = '''create temporary table %s (
		_GelLane_key	int	not null,
		is_detected	int	not null
		)''' % TMP_GELLANE_DETECTED
	dbAgnostic.execute(cmd0)
	logger.info("Created table");

	cmd1 = '''with positiveCounts as (
		select gl._GelLane_key,
			count(distinct gb._GelBand_key) as positiveCount
		from gxd_gellane gl
		left outer join gxd_gelband gb on (gl._GelLane_key = gb._GelLane_key)
		left outer join gxd_strength gs on (gb._Strength_key = gs._Strength_key)
		where gs.strength not in ('Absent', 'Not Applicable')
			or gb._GelBand_key is null
		group by 1
		)
		insert into %s
		select _GelLane_key, case
			when positiveCount = 0 then 0
			else 1
			end
		from positiveCounts''' % TMP_GELLANE_DETECTED
	dbAgnostic.execute(cmd1)
	createIndex(TMP_GELLANE_DETECTED, '_GelLane_key')
	logger.info("Filled and indexed table")

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
	#	1. assay key
	#	2. result key (in situ assays) or gel lane key (gel assays)
	#	3. structure term key
	#
	# For RNA-Seq data, consolidated_measurement_keys in 
	# expression_ht_consolidated_sample_measurement are taken directly from
	# _RNASeqCombined_key in the production-style database's 
	# GXD_HTSample_RNASeqCombined table.

	assayTypeTable = getAssayTypeSeqnumTable()
	markerTable = getMarkerSeqnumTable()
	structureTable = getStructureSeqnumTable()
	gelLaneTable = _getGelLaneTable()
	isResultTable = _getInSituResultTable()

	logger.info('Began building: %s' % TMP_KEYSTONE)

	cmd0 = '''create temporary table %s (
		uni_key			int	not null,
		is_classical		int	not null,
		result_key		int	not null,
		ageMin			float	null,
		ageMax			float	null,
		_Stage_key		int	not null,
		by_structure		int	not null,
		by_marker		int	not null,
		by_assay_type		int	not null,
		is_detected		int	not null
		)''' % TMP_KEYSTONE

	dbAgnostic.execute(cmd0)
	logger.info('Created temp table: %s' % TMP_KEYSTONE)

	cmd1 = '''with results as (
		select 1 as is_classical, a._Assay_key, r._Result_key,
			s.ageMin, s.ageMax, rs._stage_key,
			emapa.seqNum as emapaSeqNum,
			mrk.seqNum, aa.seqNum as atSeqNum, isr.is_detected
		from gxd_specimen s,
			gxd_assay a,
			gxd_insituresult r,
			gxd_isresultstructure rs,
			voc_term struct,
			%s mrk,
			%s aa,
			%s emapa,
			%s isr
		where s._Specimen_key = r._Specimen_key
			and r._Result_key = rs._Result_key
			and r._Result_key = isr._Result_key
			and s._Assay_key = a._Assay_key
			and rs._Emapa_Term_key = emapa._Term_key
			and struct._Term_key = emapa._Term_key
			and a._AssayType_key not in (10,11)
			and a._Marker_key = mrk._Marker_key
			and a._AssayType_key = aa._AssayType_key
		union
		select 1 as is_classical, g._Assay_key, g._GelLane_key,
			g.ageMin, g.ageMax, gs._stage_key,
			emapa.seqNum as emapaSeqNum,
			mrk.seqNum, aa.seqNum as atSeqNum, glk.is_detected
		from gxd_gellane g,
			gxd_gellanestructure gs,
			gxd_assay a,
			%s mrk,
			%s aa,
			%s emapa,
			%s glk
		where g._GelControl_key = 1
			and g._Assay_key = a._Assay_key
			and g._GelLane_key = gs._GelLane_key
			and g._GelLane_key = glk._GelLane_key
			and gs._emapa_term_key = emapa._Term_key
			and a._Marker_key = mrk._Marker_key
			and a._AssayType_key = aa._AssayType_key
		)
		insert into %s
		select distinct row_number() over (order by _Assay_key, _Result_key, emapaSeqNum), 
			is_classical,
			row_number() over (order by _Assay_key, _Result_key, emapaSeqNum),
			ageMin, ageMax, _stage_key, emapaSeqNum, seqNum, atSeqNum, is_detected
		from results''' % (markerTable, assayTypeTable, structureTable,
			isResultTable, markerTable, assayTypeTable,
			structureTable, gelLaneTable, TMP_KEYSTONE)

	dbAgnostic.execute(cmd1)
	classicalRows = _getRowCount(TMP_KEYSTONE)
	logger.info('Added %d classical rows' % classicalRows)

	# Get to the set of unique sample data for each consolidated sample.
	cmd2 = '''select distinct rsc._RNASeqCombined_key, s._Emapa_key,
			s._Stage_key, mrk.seqNum, s.ageMin, s.ageMax,
			case 
			when level.term = 'Below Cutoff' then 0
			else 1
			end as is_detected
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
	
	cmd3 = 'select max(seqNum) from %s' % assayTypeTable
	cols, rows = dbAgnostic.execute(cmd3)
	rnaseq = rows[0][0]

	# Insert RNA-Seq rows into the keystone table
	cmd4 = '''insert into tmp_keystone
		select distinct %d + rm._RNASeqCombined_key, 0,
			rm._RNASeqCombined_key, rm.ageMin, rm.ageMax,
			rm._Stage_key, emapa.seqNum, rm.seqNum, aa.seqNum,
			rm.is_detected
		from rscmap rm,
			%s aa,
			%s emapa
		where rm._Emapa_key = emapa._Term_key
			and 99 = aa._AssayType_key''' % (classicalRows,
				assayTypeTable, structureTable)
	dbAgnostic.execute(cmd4)

	rnaseqRows = _getRowCount(TMP_KEYSTONE, 'where by_assay_type = %d' % rnaseq)
	logger.info('Added %d RNA-Seq rows' % rnaseqRows) 

	setExists(TMP_KEYSTONE)
	logger.info('Done building: %s' % TMP_KEYSTONE)
	return

def _symbolCompare(a, b):
	# compare two tuples (marker key, symbol) by symbol, then key

	c = symbolsort.nomenCompare(a[1], b[1])
	if c == 0:
		c = cmp(a[0], b[0])
	return c

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

	toSort.sort(_symbolCompare)
	logger.info('Sorted %d marker symbols' % len(toSort))

	toLoad = []
	seqNum = 0
	for (key, symbol) in toSort:
		seqNum = seqNum + 1
		toLoad.append( (key, seqNum) )

	cmd1 = '''create temporary table %s (
		_Marker_key	int	not null,
		seqNum		int	not null
		)'''
	dbAgnostic.execute(cmd1 % TMP_MARKER_SEQNUM)
	dbAgnostic.batchInsert(TMP_MARKER_SEQNUM, toLoad)
	logger.info('Loaded table with %d rows' % _getRowCount(TMP_MARKER_SEQNUM))

	createIndex(TMP_MARKER_SEQNUM, '_Marker_key', True)

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
		_AssayType_key	int	not null,
		seqNum		int	not null
		)'''
	dbAgnostic.execute(cmd1 % TMP_ASSAYTYPE_SEQNUM)
	dbAgnostic.batchInsert(TMP_ASSAYTYPE_SEQNUM, toLoad)
	logger.info('Loaded table with %d rows' % _getRowCount(TMP_ASSAYTYPE_SEQNUM))

	createIndex(TMP_ASSAYTYPE_SEQNUM, '_AssayType_key', True)

	setExists(TMP_ASSAYTYPE_SEQNUM)
	logger.info('Done building: %s' % TMP_ASSAYTYPE_SEQNUM)
	return

def _termCompare(a, b):
	# compare two tuples (term key, seq num) by seq num, then key

	c = cmp(a[1], b[1])
	if c == 0:
		c = cmp(a[0], b[0])
	return c

def _buildStructureSeqnumTable():
	# build the (EMAPA) structure sequence number table

	logger.info('Began building: %s' % TMP_STRUCTURE_SEQNUM)

	VocabSorter.setVocabs(90)

	# get EMAPA term keys used in either high-throughput experiments or
	# classical experiments
	cmd0 = '''select t._Term_key
		from voc_term t
		where t._Vocab_key = 90
			and (exists (select(1) from gxd_htsample s
				where t._Term_key = s._Emapa_key)
				or exists (select(1) from gxd_expression e
				where t._Term_key = e._Emapa_Term_key) )'''

	cols, rows = dbAgnostic.execute(cmd0)
	keyCol = dbAgnostic.columnNumber(cols, '_Term_key')

	toSort = []
	for row in rows:
		key = row[keyCol]
		toSort.append( (key, VocabSorter.getSequenceNum(key) ) )
	logger.info('Got %d terms' % len(toSort))

	toSort.sort(_termCompare)
	logger.info('Sorted %d terms' % len(toSort))

	cmd1 = '''create temporary table %s (
		_Term_key	int	not null,
		seqNum		int	not null
		)'''
	dbAgnostic.execute(cmd1 % TMP_STRUCTURE_SEQNUM)
	dbAgnostic.batchInsert(TMP_STRUCTURE_SEQNUM, toSort)
	logger.info('Loaded table with %d rows' % _getRowCount(TMP_STRUCTURE_SEQNUM))

	createIndex(TMP_STRUCTURE_SEQNUM, '_Term_key', True)

	setExists(TMP_STRUCTURE_SEQNUM)
	logger.info('Done building: %s' % TMP_STRUCTURE_SEQNUM)
	return

