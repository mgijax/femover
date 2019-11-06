# Name: GXDUniUtils.py
# Purpose: Contains code for bringing classical expression results and RNA-Seq
#	consolidated sample measurements together into a set of universal
#	expression results.  These eventually feed into the gxdResult Solr index
#	that feeds the expression summary pages in the fewi.

import dbAgnostic
import logger

###--- Global Variables ---###

# This temporary table maps from a classical result key or an RNA-Seq
# consolidated measurement key to its corresponding universal key (uni_key).
TMP_KEYSTONE = 'tmp_keystone'

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

	logger.info('Began building: %s' % TMP_KEYSTONE)

	cmd0 = '''create temporary table %s (
		uni_key			int	not null,
		is_classical		int	not null,
		result_key		int	not null,
		ageMin			float	null,
		ageMax			float	null,
		_Stage_key		int	not null,
		_Structure_Term_key	int	not null,
		_Marker_key		int	not null,
		_AssayType_key		int	not null
		)''' % TMP_KEYSTONE

	dbAgnostic.execute(cmd0)
	logger.info('Created temp table: %s' % TMP_KEYSTONE)

	cmd1 = '''with results as (
		select 1 as is_classical, a._Assay_key, r._Result_key,
			s.ageMin, s.ageMax, rs._stage_key, struct._Term_key,
			a._Marker_key, a._AssayType_key
		from gxd_specimen s,
			gxd_assay a,
			gxd_insituresult r,
			gxd_isresultstructure rs,
			voc_term_emaps vte,
			voc_term struct
		where s._Specimen_key = r._Specimen_key
			and r._Result_key = rs._Result_key
			and s._Assay_key = a._Assay_key
			and rs._Emapa_Term_key = vte._Emapa_Term_key
			and rs._Stage_key = vte._Stage_key
			and vte._Term_key = struct._Term_key
			and a._AssayType_key not in (10,11)
		union
		select 1 as is_classical, g._Assay_key, g._GelLane_key,
			g.ageMin, g.ageMax, gs._stage_key, struct._Term_key,
			a._Marker_key, a._AssayType_key
		from gxd_gellane g,
			gxd_gellanestructure gs,
			voc_term_emaps vte,
			voc_term struct,
			gxd_assay a
		where g._GelControl_key = 1
			and g._Assay_key = a._Assay_key
			and g._GelLane_key = gs._GelLane_key
			and vte._emapa_term_key = gs._emapa_term_key
			and vte._stage_key = gs._stage_key
			and struct._term_key = vte._term_key
		)
		insert into %s
		select distinct row_number() over (order by _Assay_key, _Result_key, _Term_key), 
			is_classical,
			row_number() over (order by _Assay_key, _Result_key, _Term_key),
			ageMin, ageMax, _stage_key, _Term_key, _Marker_key,
			_AssayType_key
		from results''' % TMP_KEYSTONE

	dbAgnostic.execute(cmd1)
	classicalRows = _getRowCount(TMP_KEYSTONE)
	logger.info('Added %d classical rows' % classicalRows)

	# Get to the set of unique sample data for each consolidated sample.
	cmd2 = '''select distinct rsc._RNASeqCombined_key, s._Emapa_key,
			s._Stage_key, rsc._Marker_key, s.ageMin, s.ageMax
		into temporary table rscmap
		from gxd_htsample_rnaseq rsc, gxd_htsample s
		where rsc._Sample_key = s._Sample_key'''

	createIndex('rscmap', '_Emapa_key')
	createIndex('rscmap', '_Stage_key')
	
	# Insert RNA-Seq rows into the keystone table
	cmd3 = '''insert into tmp_keystone
		select distinct %d + rm._RNASeqCombined_key, 0,
			rm._RNASeqCombined_key, rm.ageMin, rm.ageMax,
			rm._Stage_key, vte._Term_key, rm._Marker_key, 99
		from rscmap rm,
			voc_term_emaps vte
		where vte._emapa_term_key = rm._emapa_key
			and vte._stage_key = rm._stage_key''' % classicalRows

	rnaseqRows = _getRowCount(TMP_KEYSTONE, '_AssayType_key = 99')
	logger.info('Added %d RNA-Seq rows' % rnaseqRows) 

	setExists(TMP_KEYSTONE)
	logger.info('Done building: %s' % TMP_KEYSTONE)
	return

