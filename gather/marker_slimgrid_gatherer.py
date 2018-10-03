#!/usr/local/bin/python
# 
# gathers data for the tables supporting the slimgrids on the marker detail
# page

import config
import Gatherer
import logger
import dbAgnostic
import GOFilter
import gc
import os
import VocabUtils
import GOAnnotations
from annotation import transform
from _sqlite3 import Row

# don't count GO annotations with NOT qualifiers
GOFilter.keepNots(False)

###--- Globals ---###

error = 'slimgrid.error'	# standard exception raised in this gatherer

MAX_HEADING_KEY = 0		# maximum heading_key so far
MAX_SEQUENCE_NUM = 0		# maximum heading sequence_num so far

MP_CLOSURE = 'mp_closure'			# table name for closure table for MP terms
MP_SOURCE_ANNOT = 'mp_source_annot'	# table name for MP source annotation info
MP_HEADER_KEYS = 'mp_header_keys'	# table name for MP header term keys
GO_HEADER_KEYS = 'go_header_keys'	# table name for GO header keys
GXD_HEADER_KEYS = 'gxd_header_keys'	# table name for GXD anatomy header keys
GXD_CACHE = 'gxd_cache'				# table name for GXD result cache
MARKER_CACHE = 'marker_cache'		# table name for cache of markers to process

NORMAL_QUALIFIER = 2181424	# normal qualifier for MP annotations

# name of a temp table containing filtered GO annotations per marker
GO_ANNOT_TABLE = GOFilter.getCountableAnnotationTableByMarker()

# maps from evidence keys to a list of property data (for isoforms and context)
GO_PROPERTIES_MAP = GOAnnotations.getIsoformsOfAnnotations('voc_annot',
	GOAnnotations.getContextOfAnnotations('voc_annot') )

###--- Functions ---###

def getNextHeadingKey():
	global MAX_HEADING_KEY

	MAX_HEADING_KEY = MAX_HEADING_KEY + 1
	return MAX_HEADING_KEY

def getNextSequenceNum():
	global MAX_SEQUENCE_NUM

	MAX_SEQUENCE_NUM = MAX_SEQUENCE_NUM + 1
	return MAX_SEQUENCE_NUM

def uniqueCount(rows, ignoreColumns = []):
	# Find the number of unique rows if we ignore the specified columns (by index)
	
	uniqueSet = set()
	
	if rows:
		rowLength = len(rows[0])
		for row in rows:
			subrow = []

			i = 0
			while (i < rowLength):
				if i not in ignoreColumns:
					subrow.append(row[i])
				i = i + 1

			uniqueSet.add(tuple(subrow))

			if (row[0] == 12184) and (str(row).find('1858') >= 0):
				logger.debug(str(row))
			
	return len(uniqueSet)

def setupPhenoHeaderKeys():
	# build a temp table with the keys of the MP header terms

	cmd1 = '''select _Object_key as _Term_key 
		into temporary table %s
	        from MGI_SetMember 
		where _Set_key = 1051
		''' % MP_HEADER_KEYS

	dbAgnostic.execute(cmd1)

	cmd2 = 'create unique index mp1 on %s (_Term_key)' % MP_HEADER_KEYS
	dbAgnostic.execute(cmd2)

	cmd3 = 'select count(1) from %s' % MP_HEADER_KEYS
	cols, rows = dbAgnostic.execute(cmd3)

	logger.debug('Put %d MP keys in %s' % (rows[0][0], MP_HEADER_KEYS))
	return 

def setupGOHeaderKeys():
	# build a temp table with the keys of the GO header terms

	cmd1 = '''select _Object_key as _Term_key 
		into temporary table %s
	        from MGI_SetMember 
		where _Set_key = 1050
		''' % GO_HEADER_KEYS

	dbAgnostic.execute(cmd1)

	cmd2 = 'create unique index go1 on %s (_Term_key)' % GO_HEADER_KEYS
	dbAgnostic.execute(cmd2)

	cmd3 = 'select count(1) from %s' % GO_HEADER_KEYS
	cols, rows = dbAgnostic.execute(cmd3)

	logger.debug('Put %d GO keys in %s' % (rows[0][0], GO_HEADER_KEYS))
	return 

def setupGxdHeaderKeys():
	# build a temp table with the keys of the Gxd header terms

	cmd1 = '''select _Object_key as _Term_key 
		into temporary table %s
	        from MGI_SetMember 
		where _Set_key = 1049
		''' % GXD_HEADER_KEYS

	dbAgnostic.execute(cmd1)

	cmd2 = 'create unique index gxd1 on %s (_Term_key)' % GXD_HEADER_KEYS
	dbAgnostic.execute(cmd2)

	cmd3 = 'select count(1) from %s' % GXD_HEADER_KEYS
	cols, rows = dbAgnostic.execute(cmd3)

	logger.debug('Put %d GXD keys in %s' % (rows[0][0], GXD_HEADER_KEYS))
	return 

def setupGOCacheTables():
	# create a closure table with a slice of GO terms

	cmd1 = '''select dc._AncestorObject_key, dc._DescendentObject_key
		into temporary table go_closure
		from dag_closure dc, %s ghk
		where dc._AncestorObject_key = ghk._Term_key
		''' % GO_HEADER_KEYS

	cmd2 = '''insert into go_closure
		select _Term_key, _Term_key
		from %s''' % GO_HEADER_KEYS

	cmd3 = 'create index goclo1 on go_closure (_AncestorObject_key)'
	cmd4 = 'create index goclo2 on go_closure (_DescendentObject_key)'

	for cmd in [ cmd1, cmd2, cmd3, cmd4 ]:
		dbAgnostic.execute(cmd)
	logger.debug('created GO closure temp table')
	return

def setupMPCacheTables():
	# create temp tables needed for efficient MP processing

	# MP closure table will only hold descendants of MP header terms.  We omit the
	# header terms themselves, because they will only have Normal annotations.
	cmd1 = '''select dc._AncestorObject_key, dc._DescendentObject_key
		into temporary table %s
		from dag_closure dc, %s ghk
		where dc._AncestorObject_key = ghk._Term_key''' % (MP_CLOSURE, MP_HEADER_KEYS)

	cmd2 = 'create index mpClo1 on %s (_AncestorObject_key)' % MP_CLOSURE
	cmd3 = 'create index mpClo2 on %s (_DescendentObject_key)' % MP_CLOSURE

	for cmd in [ cmd1, cmd2, cmd3 ]:
		dbAgnostic.execute(cmd)
	logger.debug('created MP closure temp table')
	
	# Create a temp table mapping from MP annotation keys to their source annotation keys.
	cmd4 = '''select va._Annot_key, vep.value::int as _Source_Annot_key
		into temporary table %s
		from voc_annot va, voc_evidence ve, voc_evidence_property vep, voc_term vp
		where va._AnnotType_key = 1015
			and va._Annot_key = ve._Annot_key
			and ve._AnnotEvidence_key = vep._AnnotEvidence_key
			and vep._PropertyTerm_key = vp._Term_key
			and vp.term = '_SourceAnnot_key' ''' % MP_SOURCE_ANNOT
			
	cmd5 = 'create index saIdx1 on %s (_Annot_key)' % MP_SOURCE_ANNOT
	cmd6 = 'create index saIdx2 on %s (_Source_Annot_key)' % MP_SOURCE_ANNOT
	
	for cmd in [ cmd4, cmd5, cmd6 ]:
		dbAgnostic.execute(cmd)
	logger.debug('created MP source annot table')
	return

def setupGxdCacheTables():
	# sets up the cache tables necessary for GXD results

	# create a temp table which maps from expression strength values to a
	# simple 0 for absent-ish values (absent, ambiguous, not specified) or
	# a 1 for present-ish values (all the others).

	cmd4 = '''select _Strength_key, case
			when _Strength_key <= 1 then 0
			when _Strength_key = 3  then 0
			else 1
			end as is_present
		into temporary table gxd_present
		from gxd_strength'''

	cmd5 = 'create unique index gp1 on gxd_present(_Strength_key)'

	for cmd in [ cmd4, cmd5 ]:
		dbAgnostic.execute(cmd)
	logger.debug('Computed strength table')

	# create a temp table which contains assay keys only for those assays
	# which are for GXD -- not recombinase assays.

	cmd5a = '''select distinct _Assay_key
		into temporary table gxd_assays
		from gxd_expression
		where isForGxd = 1'''

	cmd5b = 'create unique index gaa on gxd_assays(_Assay_key)'

	for cmd in [ cmd5a, cmd5b ]:
		dbAgnostic.execute(cmd)
	logger.debug('Built table of only GXD assays')

	# create a temp table with result data for in situ assays.  Each row
	# constitutes one expression result for an in situ assay.

	# breaking this up logically using 'with' clauses improves performance
	# from about 50 seconds down to 5 seconds

	cmd6 = '''with specimens as (
			select s._Specimen_key, s._Assay_key, s._Genotype_key
			from gxd_specimen s, gxd_assays gaa
			where s._Assay_key = gaa._Assay_key
			),
		results as (select r._Specimen_key, r._Strength_key,
				r._Result_key, rs._emapa_term_Key, rs._stage_key
			from gxd_insituresult r, gxd_isresultstructure rs
			where r._Result_key = rs._Result_key
		)
		select s._Assay_key, s._Genotype_key, p.is_present,
			r._Result_key, r._emapa_term_key, r._stage_key,
			vte._term_key as _emaps_key, s._Specimen_key
		into temporary table gxd_insitu_results
		from specimens s, results r, gxd_present p,
		voc_term_emaps vte
		where s._Specimen_key = r._Specimen_key
		and r._Strength_key = p._Strength_key
		and vte._emapa_term_key = r._emapa_term_key
		and vte._stage_key = r._stage_key
		'''

	cmd7 = 'create index isr1 on gxd_insitu_results (_emaps_key)'
	cmd8 = 'create index isr2 on gxd_insitu_results (_Assay_key)'

	for cmd in [ cmd6, cmd7, cmd8 ]:
		dbAgnostic.execute(cmd)
	logger.debug('Computed in situ result table')

	# create a temp table with result data for gel assays.  An expression
	# result consists of a unique lane/structure pair, computing the
	# strength as the strongest result from the bands in that lane.  For
	# the purposes of the slimgrids, we only really care about whether a
	# strength is (absent, ambiguous, not specified) or is otherwise.

	# again, using "with" to group tables shrinks the query time
	# substantially (8 seconds to 4)
	cmd9 = '''with lanes as (
			select g._Assay_key, g._Genotype_key, g._GelLane_key,
				gs._emapa_term_key, gs._stage_key
			from gxd_gellane g, gxd_gellanestructure gs,
				gxd_assays ga
			where ga._Assay_key = g._Assay_key
				and g._GelLane_key = gs._GelLane_key
				and g._GelControl_key = 1
			)
		select g._Assay_key, g._Genotype_key, g._GelLane_key,
			g._emapa_term_key, g._stage_key, vte._term_key as _emaps_key,
			max(p.is_present) as is_present
		into temporary table gxd_gel_results
		from lanes g, gxd_gelband b, gxd_present p,
			voc_term_emaps vte
		where g._GelLane_key = b._GelLane_key
			and b._Strength_key = p._Strength_key
			and vte._emapa_term_key = g._emapa_term_key
			and vte._stage_key = g._stage_key
		group by 1, 2, 3, 4, 5, 6'''

	cmd10 = 'create index gg1 on gxd_gel_results (_emaps_key)'
	cmd11 = 'create index gg2 on gxd_gel_results (_Assay_key)'

	for cmd in [ cmd9, cmd10, cmd11 ]:
		dbAgnostic.execute(cmd)
	logger.debug('Computed gel result table')

	# consolidate the in situ and the gel tables into a single one

	cmd12 = '''create temporary table %s (
		_Marker_key	int	not null,
		_Assay_key	int	not null,
		_Genotype_key	int	not null,
		_Emaps_key	int	not null,
		is_present	int	not null,
		is_wildtype	int	not null
		)''' % GXD_CACHE

	cmd13 = '''insert into %s
		select ga._Marker_key, isr._Assay_key, isr._Genotype_key,
			isr._Emaps_key, isr.is_present, 0
		from gxd_insitu_results isr,
			gxd_assay ga
		where isr._Assay_key = ga._Assay_key''' % GXD_CACHE

	cmd14 = '''insert into %s
		select ga._Marker_key, ggr._Assay_key, ggr._Genotype_key,
			ggr._Emaps_key, ggr.is_present, 0
		from gxd_gel_results ggr,
			gxd_assay ga
		where ggr._Assay_key = ga._Assay_key''' % \
				GXD_CACHE

	cmd15 = 'create index gc1 on %s (_Marker_key)' % GXD_CACHE
	cmd16 = 'create index gc2 on %s (_Emaps_key)' % GXD_CACHE
	cmd17 = 'create index gc3 on %s (is_present)' % GXD_CACHE
	cmd17a = 'create index gca on %s (_Assay_key)' % GXD_CACHE
	cmd17b = 'create index gcb on %s (_Genotype_key)' % GXD_CACHE

	for cmd in [ cmd12, cmd13, cmd14, cmd15, cmd16, cmd17, cmd17a, cmd17b ]:
		dbAgnostic.execute(cmd)
	logger.debug('Built combined result table')

	# flag two types of wild-type assays:
	# 1. genotypes with no allele pairs
	# 2. four part rule required:
	#    a. assay type = In situ reporter (knock in) (key 9)
	#    b. only one allele pair
	#    c. allele pair has one wild-type allele and one mutant allele
	#    d. both alleles are for the assayed gene
	# 3. not-specified genotypes (assume these to be wild-type)

	cmd18 = '''update %s
		set is_wildtype = 1
		where _Genotype_key in (select gg._Genotype_key
			from gxd_genotype gg
			where gg._Genotype_key >= 0
			and not exists (select 1 from gxd_allelepair p
				where gg._Genotype_key = p._Genotype_key))''' \
					% GXD_CACHE

	cmd19 = '''with alleles as (select _Allele_key, isWildType,
				_Marker_key
			from all_allele
		),
		genotypes as (select g._Genotype_key, g._Allele_key
			from gxd_allelegenotype g
			where g._Genotype_key >= 0
			and not exists (select 1 from gxd_allelepair gap
				where g._Genotype_key = gap._Genotype_key
				and gap.sequenceNum > 1)
		),
		assays as (select s._Assay_key, s._Genotype_key
			from gxd_specimen s,
				gxd_assay a,
				genotypes gag1,
				genotypes gag2,
				alleles a1,
				alleles a2
			where s._Assay_key = a._Assay_key
				and a._AssayType_key = 9
				and s._Genotype_key = gag1._Genotype_key
				and gag1._Allele_key = a1._Allele_key
				and a1.isWildType = 1
				and s._Genotype_key = gag2._Genotype_key
				and gag2._Allele_key = a2._Allele_key
				and a2.isWildType = 0
				and a1._Marker_key = a._Marker_key
				and a2._Marker_key = a._Marker_key
		)
		update %s gc
		set is_wildtype = 1
		from assays a
		where gc._Assay_key = a._Assay_key
			and gc._Genotype_key = a._Genotype_key''' % GXD_CACHE

	cmd19a = '''update %s
		set is_wildtype = 1
		where _Genotype_key <= 0''' % GXD_CACHE

	cmd20 = 'create index gc4 on %s (is_wildtype)' % GXD_CACHE
	cmd20a = '''update gxd_cache
		set is_present = 0
		where is_wildtype = 0'''
	cmd20b = 'analyze %s' % GXD_CACHE

	for cmd in [ cmd18, cmd19, cmd19a, cmd20, cmd20a, cmd20b ]:
		dbAgnostic.execute(cmd)
	logger.debug('Flagged wild-type assays')

	# clean up now-unneeded temp tables

	cmd21 = 'drop table gxd_insitu_results'
	cmd23 = 'drop table gxd_present'
	cmd24 = 'drop table gxd_gel_results'

	for cmd in [ cmd21, cmd23, cmd24 ]:
		dbAgnostic.execute(cmd)
	logger.debug('Removed unneded temp tables')

	# create a closure table which ties EMAPA header terms to their
	# corresponding EMAPS header terms and the descendents below those
	# EMAPS headers.  (This should ensure that we respect stage ranges
	# when rolling up annotations.)  Also includes the EMAPS headers as
	# descendents of themselves to avoid having to use a union later on.

	cmd25 = '''with emaps_headers as (
			select ghk._Term_key as _Emapa_Header_key,
				e._Term_key as _Emaps_Header_key
			from %s ghk, voc_term_emaps e
			where ghk._Term_key = e._Emapa_Term_key
		)
		select eh._Emapa_Header_key, eh._Emaps_Header_key,
			dc._DescendentObject_key as _Emaps_key
		into temporary table gxd_closure
		from dag_closure dc, emaps_headers eh
		where eh._Emaps_Header_key = dc._AncestorObject_key''' % \
			GXD_HEADER_KEYS

	cmd26 = '''insert into gxd_closure
		select ghk._Term_key, e._Term_key, e._Term_key
		from %s ghk, voc_term_emaps e
		where ghk._Term_key = e._Emapa_Term_key''' % GXD_HEADER_KEYS

	cmd27 = 'create index clo1 on gxd_closure (_Emapa_Header_key)'
	cmd28 = 'create index clo2 on gxd_closure (_Emaps_Header_key)'
	cmd28a = 'create index clo3 on gxd_closure (_Emaps_key)'

	for cmd in [ cmd25, cmd26, cmd27, cmd28, cmd28a ]:
		dbAgnostic.execute(cmd)
	logger.debug('created GXD closure temp table')
	return

def setupMarkerCacheTable():
	# set up a cache table (MARKER_CACHE) containing only marker keys that have 
	# MP, GXD, or GO annotations.  Primary key will be computed, then we can 
	# iterate over that field and avoid all the marker keys without annotations.
	
	cmd0 = '''select row_number() over (order by m._Marker_key) as row_num,
			m._Marker_key
		into temp %s
		from mrk_marker m
		where m._Marker_Status_key = 1
			and m._Organism_key = 1
			and (
				exists (select 1 from voc_annot mp
					where m._Marker_key = mp._Object_key
					and mp._AnnotType_key = 1015)
				or exists (select 1 from voc_annot go
					where m._Marker_key = go._Object_key
					and go._AnnotType_key = 1000)
				or exists (select 1 from %s gxd
					where m._Marker_key = gxd._Marker_key)
			)''' % (MARKER_CACHE, GXD_CACHE)
			
	cmd1 = 'create index mIndex1 on %s (row_num)' % MARKER_CACHE
	cmd2 = 'create index mIndex2 on %s (_Marker_key)' % MARKER_CACHE
	cmd3 = 'select min(_Marker_key) as min_key, max(_Marker_key) as max_key, count(1) as ct from %s' % MARKER_CACHE

	for cmd in [ cmd0, cmd1, cmd2 ]:
		dbAgnostic.execute(cmd)
		
	(cols, rows) = dbAgnostic.execute(cmd3)

	minKey = rows[0][dbAgnostic.columnNumber(cols, 'min_key')]
	maxKey = rows[0][dbAgnostic.columnNumber(cols, 'max_key')]
	count = rows[0][dbAgnostic.columnNumber(cols, 'ct')] 
	
	logger.debug('Cached %d marker keys from %d to %d' % (count, minKey, maxKey))
	return 

###--- Classes ---###

class HeadingCollection:
	# Is: a collection of the headings for a slimgrid
	# Has: data about the slimgrid's headings (keys, IDs, names, etc.)
	# Does: gathers data from the database and provides easy methods for
	#	retrieving the data

	def __init__ (self):
		self.data = {}
		self.terms = {}		# heading key -> [ (term key, ID), ... ]
		self.initialize()
		return

	def initialize (self):
		raise error, 'Must define initialize() method in subclass'

	def store (self, headingKey, term, termAbbrev, termKey, termID,
		gridName, gridAbbrev, seqNum):

		if not self.data.has_key(headingKey):
			self.data[headingKey] = [ headingKey, term,
				termAbbrev, gridName, gridAbbrev, seqNum ] 

		if not self.terms.has_key(headingKey):
			self.terms[headingKey] = [ (termKey, termID) ]

		elif (termKey, termID) not in self.terms[headingKey]:
				self.terms[headingKey].append (
					(termKey, termID) )
		return

	def getSeqNum (self, headingKey):
		if self.data.has_key(headingKey):
			return self.data[headingKey][-1]
		return 99999

	def getHeadingRows (self):
		rows = self.data.values()
		rows.sort(lambda a,b : cmp(a[-1], b[-1]))
		return rows

	def getTermRows (self):
		headingKeys = self.terms.keys()
		headingKeys.sort()

		rows = []

		for heading in headingKeys:
			for (termKey, termID) in self.terms[heading]:
				rows.append ( [heading, termKey, termID] )
		return rows

	def getMapping (self):
		# get a list of tuples, each being (term key, heading key),
		# ordered properly by sequence num

		rows = self.getHeadingRows()
		out = []

		for row in rows:
			heading = row[0]

			if self.terms.has_key(heading):
				for (termKey, termID) in self.terms[heading]:
					out.append ( (termKey, heading) )
		return out
	
class MPHeadingCollection (HeadingCollection):
	# Is: a HeadingCollection for the MP (Mammalian Phenotype) slimgrid

	def initialize (self):
		cmd = '''select s.label as termAbbrev,
		                t.term,
				t._Term_key,
				a.accID,
				t.sequenceNum,
				v.name as slimgrid
			from mgi_setmember s,
			     voc_term t,
			     voc_vocab v,
		   	     acc_accession a
			where s._set_key = 1051
			      and s._object_key = t._term_key
			      and t._vocab_key = v._vocab_key
			      and t._Term_key = a._Object_key
			      and a.private = 0
			      and a.preferred = 1
			      and a._MGIType_key = 13
			order by t.sequenceNum'''

		(cols, rows) = dbAgnostic.execute(cmd)

		termAbbrevCol = dbAgnostic.columnNumber (cols, 'termAbbrev')
		termCol = dbAgnostic.columnNumber (cols, 'term')
		keyCol = dbAgnostic.columnNumber (cols, '_Term_key')
		idCol = dbAgnostic.columnNumber (cols, 'accID')
		slimgridCol = dbAgnostic.columnNumber (cols, 'slimgrid')

		slimgridAbbrev = 'MP'

		for row in rows:
			termAbbrev = row[termAbbrevCol]
			accID = row[idCol]
			termKey = row[keyCol]
			term = row[termCol].strip()
			slimgrid = row[slimgridCol]

			if str(termAbbrev) == 'None':
			    termAbbrev = term

			self.store(getNextHeadingKey(), term, termAbbrev,
				termKey, accID,
				slimgrid, slimgridAbbrev, getNextSequenceNum())

		logger.debug('Got %d MP headers' % len(rows))
		return

class GOHeadingCollection (HeadingCollection):
	# Is: a HeadingCollection for the GO (Gene Ontology) slimgrids

	def initialize (self):
		cmd = '''select s.label as termAbbrev,
		                t.term,
				t._Term_key,
				a.accID,
				t.sequenceNum,
				dd.name as slimgrid,
				dd.abbreviation as slimgridAbbrev
			from mgi_setmember s,
			     voc_term t,
			     voc_vocab v,
		   	     acc_accession a,
			     dag_node dn,
			     dag_dag dd
			where s._set_key = 1050
			      and s._object_key = t._term_key
			      and t._Vocab_key = v._Vocab_key
                              and t._Term_key = dn._Object_key
			      and dn._DAG_key = dd._DAG_key
			      and dd._MGIType_key = 13 
			      and t._Term_key = a._Object_key
			      and a.private = 0
			      and a.preferred = 1
			      and a._MGIType_key = 13
			order by t.sequenceNum'''

		(cols, rows) = dbAgnostic.execute(cmd)

		termAbbrevCol = dbAgnostic.columnNumber (cols, 'termAbbrev')
		termCol = dbAgnostic.columnNumber (cols, 'term')
		keyCol = dbAgnostic.columnNumber (cols, '_Term_key')
		idCol = dbAgnostic.columnNumber (cols, 'accID')
		slimgridCol = dbAgnostic.columnNumber (cols, 'slimgrid')
		slimgridAbbrevCol = dbAgnostic.columnNumber (cols, 'slimgridAbbrev')

		for row in rows:
			termAbbrev = row[termAbbrevCol]
			accID = row[idCol]
			termKey = row[keyCol]
			term = row[termCol].strip()
			slimgrid = row[slimgridCol]
			slimgridAbbrev = row[slimgridAbbrevCol]

			if str(termAbbrev) == 'None':
			    termAbbrev = term

			self.store(getNextHeadingKey(), term, termAbbrev,
				termKey, accID,
				slimgrid, slimgridAbbrev, getNextSequenceNum())

		logger.debug('Got %d Gene Ontology headers' % len(rows))
		return

class GxdHeadingCollection (HeadingCollection):
	# Is: a HeadingCollection for the GXD anatomy slimgrid

	def initialize (self):
		cmd = '''select s.label as termAbbrev,
		                t.term,
				t._Term_key,
				a.accID,
				t.sequenceNum
			from mgi_setmember s,
			     voc_term t,
		   	     acc_accession a
			where s._set_key = 1049
			      and s._object_key = t._term_key
			      and t._Term_key = a._Object_key
			      and a.private = 0
			      and a.preferred = 1
			      and a._MGIType_key = 13
			order by t.sequenceNum'''

		(cols, rows) = dbAgnostic.execute(cmd)

		termAbbrevCol = dbAgnostic.columnNumber (cols, 'termAbbrev')
		termCol = dbAgnostic.columnNumber (cols, 'term')
		keyCol = dbAgnostic.columnNumber (cols, '_Term_key')
		idCol = dbAgnostic.columnNumber (cols, 'accID')

		slimgrid = 'Anatomy'
		slimgridAbbrev = 'Anatomy'

		for row in rows:
			termAbbrev = row[termAbbrevCol]
			accID = row[idCol]
			termKey = row[keyCol]
			term = row[termCol].strip()

			if str(termAbbrev) == 'None':
			    termAbbrev = term

			self.store(getNextHeadingKey(), term, termAbbrev,
				termKey, accID,
				slimgrid, slimgridAbbrev, getNextSequenceNum())

		logger.debug('Got %d Anatomy headers' % len(rows))
		return

class SlimgridGatherer (Gatherer.CachingMultiFileGatherer):
	# Is: a data gatherer for the slimgrid tables
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for slimgrids for
	#	the marker detail page, collates results, writes tab-delimited
	#	text file

	def preprocessCommands(self):
		"""
		Pre-processing & initialization queries
		"""
		
		# custom GO setup
		setupGOHeaderKeys()
		setupGOCacheTables()
		self.setGOHeaderCollection(GOHeadingCollection())
		
		# custom MP setup
		self.setMPHeaderCollection(MPHeadingCollection())
		setupMPCacheTables()
		
		# custom anatomy setup
		setupGxdHeaderKeys()
		setupGxdCacheTables()
		self.setGxdHeaderCollection(GxdHeadingCollection())
		
		# build the cache table of markers with annotations
		setupMarkerCacheTable()
		
		# We can only set up the chunking once we've built the cache of markers.
		self.setupChunking (
			'select min(row_num) from %s' % MARKER_CACHE,
			'select max(row_num) from %s' % MARKER_CACHE,
			10000)

	def saveHeadings (self, headerRows):
		# save the given set of header rows to their table file

		self.addRows('marker_grid_heading', headerRows)
		return 

	def saveTermMapping (self, termRows):
		# save the mapping (join table) between grid headings and
		# vocabulary terms

		self.addRows('marker_grid_heading_to_term', termRows)
		return

	def setMPHeaderCollection (self, mph):
		# notify this gatherer of an MPHeaderCollection object

		self.mpHeaderCollection = mph
		self.saveHeadings(mph.getHeadingRows())
		self.saveTermMapping(mph.getTermRows())
		setupPhenoHeaderKeys()
		return

	def setGOHeaderCollection (self, goh):
		# notify this gatherer of an GOHeaderCollection object

		self.goHeaderCollection = goh
		self.saveHeadings(goh.getHeadingRows())
		self.saveTermMapping(goh.getTermRows())
		return

	def setGxdHeaderCollection (self, gh):
		# notify this gatherer of an GxdHeaderCollection object

		self.gxdHeaderCollection = gh
		self.saveHeadings(gh.getHeadingRows())
		self.saveTermMapping(gh.getTermRows())
		return

	def processMP (self):
		# process results from the MP query

		cols, rows = self.results[0]

		markerCol = dbAgnostic.columnNumber (cols, '_Object_key')
		headerCol = dbAgnostic.columnNumber (cols, '_Term_key')
		countCol = dbAgnostic.columnNumber (cols, 'annot_count')

		byMarker = {}	# marker key -> term key -> count

		# collect the non-zero counts from the database

		for row in rows:
			markerKey = row[markerCol]
			headerKey = row[headerCol]
			value = row[countCol]

			if not byMarker.has_key(markerKey):
				byMarker[markerKey] = { headerKey : value }
			else:
				byMarker[markerKey][headerKey] = value

		# write the counts to the file, filling in zero counts for
		# headers not associated with this marker

		markerKeys = byMarker.keys()
		markerKeys.sort()

		mapping = self.mpHeaderCollection.getMapping()

		ct = 0
		for markerKey in markerKeys:
			for (termKey, headingKey) in mapping:
				seqNum = self.mpHeaderCollection.getSeqNum(
					headingKey)

				if byMarker[markerKey].has_key(termKey):
					count = byMarker[markerKey][termKey]
					color = 1
				else:
					count = 0
					color = 0

				self.addRow(self.cellTable, [ markerKey,
					headingKey, color, count, seqNum ] )
				ct = ct + 1

		logger.debug('Saved %d MP cells (%d non-zero) for %d markers' \
			% (ct, len(rows), len(byMarker)) )

		del byMarker
		del markerKeys
		gc.collect()
		return

	def processGO (self):
		# process results for the GO query

		cols, rows = self.results[1]

		rowsByMarker = {}	# marker key -> annotated term key -> list of rows
		byMarker = {}		# marker key -> annotated term key -> count

		# group/roll up annotations -- returns a dictionary with keys like:
		#	(marker key, annotated term key, qualifier, evidence code, inferred from, isoform/context value) 
		# Values in the dictionary are lists of data rows matching that key.
		annotGroupsMap = transform.groupAnnotations(cols, rows, propertiesMap=GO_PROPERTIES_MAP)
		
		headerTermKeyCol = dbAgnostic.columnNumber(cols, 'header_term_key')

		# columns to ignore when counting distinct rows
		toIgnore = [ 
			headerTermKeyCol,
			dbAgnostic.columnNumber(cols, '_AnnotEvidence_key'),
			dbAgnostic.columnNumber(cols, 'annotType'),
			dbAgnostic.columnNumber(cols, '_DAG_key'),
			] 
		
		cols.append('properties')
		for key in annotGroupsMap.keys():
			markerKey, termKey, qualifier, evidenceCode, inferredFrom, propKey = key
			groupRows = map(list, annotGroupsMap[key])
			
			headerTermKeys = []
			for row in groupRows:
				headerTermKey = row[headerTermKeyCol]
				if headerTermKey not in headerTermKeys:
					headerTermKeys.append(headerTermKey)
					
				# append property value to each row
				row.append(propKey)
			
			if markerKey not in rowsByMarker:
				rowsByMarker[markerKey] = {}
				
			for headerTermKey in headerTermKeys:
				if headerTermKey not in rowsByMarker[markerKey]:
					rowsByMarker[markerKey][headerTermKey] = groupRows
				else:
					rowsByMarker[markerKey][headerTermKey] = groupRows + rowsByMarker[markerKey][headerTermKey]

		for markerKey in rowsByMarker.keys():
			byMarker[markerKey] = {}
			for headerTermKey in rowsByMarker[markerKey].keys():
				byMarker[markerKey][headerTermKey] = uniqueCount(rowsByMarker[markerKey][headerTermKey], toIgnore)
				
		# write the counts to the file, filling in zero counts for
		# headers not associated with this marker

		markerKeys = byMarker.keys()
		markerKeys.sort()

		mapping = self.goHeaderCollection.getMapping()

		headingKeys = []
		headingTerms = {}

		for (termKey, headingKey) in mapping:
			if headingTerms.has_key(headingKey):
				headingTerms[headingKey].append(termKey)
			else:
				headingTerms[headingKey] = [ termKey ]
				headingKeys.append(headingKey)

		ct = 0
		for markerKey in markerKeys:
			# need to consolidate cases where there are multiple
			# terms for a single heading

			for hKey in headingKeys:
				seqNum = self.goHeaderCollection.getSeqNum(hKey)
				color = 0
				count = 0
				
				for tKey in headingTerms[hKey]:
					if byMarker[markerKey].has_key(tKey):
						count = count + \
						    byMarker[markerKey][tKey]
				if count > 0:
					color = 1

				self.addRow(self.cellTable, [ markerKey,
					hKey, color, count, seqNum ] )

				ct = ct + 1

		logger.debug('Saved %d GO cells (%d non-zero) for %d markers' \
			% (ct, len(rows), len(byMarker)) )

		del byMarker
		del markerKeys
		gc.collect()
		return

	def processAnatomy (self):
		# process the anatomy results in queries 2 & 3

		counts = {}		# marker key -> term key -> annot count
		colors = {}		# marker key -> term key -> color code

		# collect the non-zero counts from the database

		cols, rows = self.results[2]

		markerCol = dbAgnostic.columnNumber(cols, '_Marker_key')
		headerCol = dbAgnostic.columnNumber(cols, '_Term_key')
		countCol = dbAgnostic.columnNumber(cols, 'annot_count')
		presentCol = dbAgnostic.columnNumber(cols, 'some_present')
		wildtypeCol = dbAgnostic.columnNumber(cols, 'some_wildtype')

		for row in rows:
			markerKey = row[markerCol]
			headerKey = row[headerCol]
			value = row[countCol]
			somePresent = row[presentCol]
			someWildtype = row[wildtypeCol]

			if somePresent and someWildtype:
				color = 1
			else:
				color = 2

			if not counts.has_key(markerKey):
				counts[markerKey] = { headerKey : value }
				colors[markerKey] = { headerKey : color }
			else:
				counts[markerKey][headerKey] = value
				colors[markerKey][headerKey] = color

		# so we now have the counts in 'counts' and the color values in
		# 'colors'.  When we write the info to the file, we need to
		# fill in zero counts for headers not associated with this
		# marker.

		markerKeys = counts.keys()
		markerKeys.sort()

		mapping = self.gxdHeaderCollection.getMapping()

		ct = 0

		for markerKey in markerKeys:
			for (termKey, headingKey) in mapping:
			    seqNum = self.gxdHeaderCollection.getSeqNum(
				headingKey)

			    if counts[markerKey].has_key(termKey):
				count = counts[markerKey][termKey]
				color = colors[markerKey][termKey]
			    else:
				count = 0
				color = 0

			    self.addRow(self.cellTable, [ markerKey,
					headingKey, color, count, seqNum ] )
			    ct = ct + 1

		logger.debug('Saved %d anatomy cells for %d markers' % (ct,
			len(markerKeys)) )

		del counts
		del colors
		del markerKeys
		del mapping
		gc.collect()

		return

	def collateResults (self):
		# override standard method for processing results

		self.cellTable = 'marker_grid_cell'
		self.processMP()
		self.processGO()
		self.processAnatomy()
		return 

###--- globals ---###

cmds = [
	# 0. counts of annotations to each MP header for each marker.  Note
	# that we intentionally ignore direct annotations to header terms 
	# (because they are all 'normal' annotations) and only include those
	# we reach via the DAG.  We also exclude annotations with a normal
	# qualifier.  Note that we want to count the genotype-level
	# annotations that were rolled-up to the marker, so we need to count
	# the values for the _SourceAnnot_key property.
	'''
	with markers as (
		select _Marker_key
		from %s
		where row_num >= %%d
			and row_num < %%d
	),
	annotations as (
		select va._Object_key, dc._AncestorObject_key as _Term_key, src._Source_Annot_key as _Annot_key,
			va._Term_key as annotated_term_key
		from voc_annot va, %s dc, %s src, markers m
		where va._AnnotType_key = 1015
			and va._Object_key = m._Marker_key
			and va._Term_key = dc._DescendentObject_key
			and va._Qualifier_key != 2181424
			and va._Annot_key = src._Annot_key
	)
	select _Object_key, _Term_key,
		count(distinct annotated_term_key) as annot_count
	from annotations
	group by _Object_key, _Term_key''' % (MARKER_CACHE, MP_CLOSURE, MP_SOURCE_ANNOT),

	# 1. A unique GO annotation for a marker is a unique set of (marker
	# key, term, qualifier, evidence code, and inferred-from value).  When
	# we count these, we must also exclude any unwanted No Data annotations
	# (those annotations with an ND evidence code, where the annotated
	# marker has another annotation within the same DAG).  Need to get 
	# annotations to ancestors via the DAG, plus annotations directly to
	# the header terms themselves.
	
	# TBD -- Do we need to also include the isoform or context when defining a distinct annotation?
	#	Yes, both.
	
	'''
	with markers as (
		select _Marker_key
		from %s
		where row_num >= %%d
			and row_num < %%d
	)
	select distinct k._Marker_key as _Object_key, 'GO/Marker' as annotType, k.inferredFrom,
		k._Qualifier_key, k._EvidenceTerm_key, k._Term_key,
		e._AnnotEvidence_key, gh._Term_key as header_term_key, k._DAG_key
	from go_header_keys gh, go_closure dc, keepers k, markers m, voc_evidence e
	where gh._Term_key = dc._AncestorObject_key
		and dc._DescendentObject_key = k._Term_key
		and k._EvidenceTerm_key = e._EvidenceTerm_key
		and (k.inferredFrom = e.inferredFrom or (k.inferredFrom is null and e.inferredFrom is null))
		and k._Marker_key = m._Marker_key
		and k._Annot_key = e._Annot_key''' % MARKER_CACHE,	

	# 2. Collate the GXD expression annotations for anatomy headers.  We
	# want the count of all annotations, plus the flags for whether there
	# are any wild-type and present annotations.  If either of those flags
	# are missing, then we don't want to show the cell with a positive
	# annotation color (because we don't want to show only annotations to
	# mutatnt genotypes as being present).  Relies on gxd_closure
	# containing only header terms as ancestors.
	'''
	with markers as (
		select _Marker_key
		from %s
		where row_num >= %%d
			and row_num < %%d
	),
	annotations as (
		select dc._Emapa_Header_key as _Term_key, gc._Marker_key,
			gc._Assay_key, gc._Genotype_key, gc._Emaps_key,
			gc.is_present, gc.is_wildtype
		from gxd_closure dc, markers m, gxd_cache gc
		where dc._Emaps_key = gc._Emaps_key
			and gc._Marker_key = m._Marker_key
	)
	select _Marker_key, _Term_key,
		count(1) as annot_count,
		max(is_present) as some_present,
		max(is_wildtype) as some_wildtype
	from annotations
	group by _Marker_key, _Term_key''' % MARKER_CACHE,
	]

files = [ ('marker_grid_cell',
		[ 'marker_key', 'heading_key', 'color_level', 'value',
			'sequence_num' ],
		[ Gatherer.AUTO, 'marker_key', 'heading_key', 'color_level',
			'value', 'sequence_num' ] ),
	('marker_grid_heading',
		[ 'heading_key', 'heading', 'heading_abbreviation',
		'grid_name', 'grid_name_abbreviation', 'sequence_num' ],
		[ 'heading_key', 'heading', 'heading_abbreviation',
		'grid_name', 'grid_name_abbreviation', 'sequence_num' ],
		),
	('marker_grid_heading_to_term',
		[ 'heading_key', 'term_key', 'term_id' ],
		[ Gatherer.AUTO, 'heading_key', 'term_key', 'term_id' ],
		),
	]

#
# for testing
#	'select min(_Marker_key) + 10 from MRK_Marker where _Organism_key = 1',
#

# global instance of a SlimgridGatherer
gatherer = SlimgridGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
