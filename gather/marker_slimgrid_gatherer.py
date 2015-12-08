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

# don't count GO annotations with NOT qualifiers
GOFilter.keepNots(False)

###--- Globals ---###

error = 'slimgrid.error'	# standard exception raised in this gatherer

MAX_HEADING_KEY = 0		# maximum heading_key so far
MAX_SEQUENCE_NUM = 0		# maximum heading sequence_num so far

MP_HEADER_KEYS = 'mp_header_keys'	# table name for MP header term keys
GO_HEADER_KEYS = 'go_header_keys'	# table name for GO header keys
GXD_HEADER_KEYS = 'gxd_header_keys'	# table name for GXD anatomy header keys
GXD_CACHE = 'gxd_cache'			# table name for GXD result cache

NORMAL_QUALIFIER = 2181424	# normal qualifier for MP annotations

# name of a temp table containing filtered GO annotations per marker
GO_ANNOT_TABLE = GOFilter.getCountableAnnotationTableByMarker()

###--- Functions ---###

def getNextHeadingKey():
	global MAX_HEADING_KEY

	MAX_HEADING_KEY = MAX_HEADING_KEY + 1
	return MAX_HEADING_KEY

def getNextSequenceNum():
	global MAX_SEQUENCE_NUM

	MAX_SEQUENCE_NUM = MAX_SEQUENCE_NUM + 1
	return MAX_SEQUENCE_NUM

def setupPhenoHeaderKeys(mph):
	# build a temp table with the keys of the MP header terms

	cmd1 = '''create temporary table %s (
		_Term_key	int	not null,
		primary key(_Term_Key) )''' % MP_HEADER_KEYS

	cmd2 = 'insert into %s values (%d)'

	dbAgnostic.execute(cmd1)
	for (termKey, headingKey) in mph.getMapping():
		dbAgnostic.execute(cmd2 % (MP_HEADER_KEYS, termKey))

	logger.debug('Found %d MP header keys' % len(mph.getMapping()))
	return 

def setupPhenoHeaderKeysOld():
	# build a temp table with the keys of the MP header terms

	cmd1 = '''select t._Term_key
		into temporary table %s
		from voc_term t, voc_vocab v
		where t._Vocab_key = v._Vocab_key
		and v.name = 'Mammalian Phenotype'
		and t.sequenceNum is not null''' % MP_HEADER_KEYS

	dbAgnostic.execute(cmd1)

	cmd2 = 'create unique index mph1 on %s (_Term_key)' % MP_HEADER_KEYS
	dbAgnostic.execute(cmd2)

	cmd3 = 'select count(1) from %s' % MP_HEADER_KEYS
	cols, rows = dbAgnostic.execute(cmd3)

	logger.debug('Found %d MP header keys' % rows[0][0])
	return 

def setupGOHeaderKeys():
	# build a temp table with the keys of the GO header terms

	cmd1 = '''select t._Term_key
		into temporary table %s
		from voc_term t, voc_vocab v
		where t._Vocab_key = v._Vocab_key
		and v.name = 'GO'
		and t.sequenceNum is not null''' % GO_HEADER_KEYS

	dbAgnostic.execute(cmd1)

	cmd2 = 'create unique index go1 on %s (_Term_key)' % GO_HEADER_KEYS
	dbAgnostic.execute(cmd2)

	cmd3 = 'select count(1) from %s' % GO_HEADER_KEYS
	cols, rows = dbAgnostic.execute(cmd3)

	logger.debug('Put %d GO keys in %s' % (rows[0][0], GO_HEADER_KEYS))
	return 

def setupGxdHeaderKeys():
	# build a temp table with the keys of the Gxd header terms

	cmd1 = '''select t._Term_key
		into temporary table %s
		from voc_term t, voc_vocab v
		where t._Vocab_key = v._Vocab_key
		and v.name = 'EMAPA'
		and t.sequenceNum is not null''' % GXD_HEADER_KEYS

	dbAgnostic.execute(cmd1)

	cmd2 = 'create unique index gxd1 on %s (_Term_key)' % GXD_HEADER_KEYS
	dbAgnostic.execute(cmd2)

	cmd3 = 'select count(1) from %s' % GXD_HEADER_KEYS
	cols, rows = dbAgnostic.execute(cmd3)

	logger.debug('Put %d GXD keys in %s' % (rows[0][0], GXD_HEADER_KEYS))
	return 

def setupGOCacheTables():
	# create a closure table with a slice of EMAPA terms

	cmd1 = '''select dc._AncestorObject_key, dc._DescendentObject_key
		into temporary table go_closure
		from dag_closure dc, %s ghk
		where dc._AncestorObject_key = ghk._Term_key''' % \
			GO_HEADER_KEYS

	cmd2 = '''insert into go_closure
		select _Term_key, _Term_key
		from %s''' % GO_HEADER_KEYS

	cmd3 = 'create index goclo1 on go_closure (_AncestorObject_key)'
	cmd4 = 'create index goclo2 on go_closure (_DescendentObject_key)'

	for cmd in [ cmd1, cmd2, cmd3, cmd4 ]:
		dbAgnostic.execute(cmd)
	logger.debug('created GO closure temp table')
	return

def setupGxdCacheTables():
	# sets up the cache tables necessary for GXD results

	# create mapping table from an AD structure key to its corresponding
	# EMAPS and EMAPA keys

	# Using these 'with' clauses is a little more cumberson, but the
	# performance improves dramatically. (65 seconds vs. 0.5 second)
	cmd0 = '''with ad as (
			select m._mapping_key, d._Object_key as _Structure_key
			from mgi_emaps_mapping m, acc_accession d
			where m.accID = d.accID
			and d._MGIType_key = 38
		),
		emaps as (
			select m._mapping_key, e._Object_key as _Emaps_key
			from mgi_emaps_mapping m, acc_accession e
			where m.emapsID = e.accID
			and e._MGIType_key = 13
		)
		select a._Structure_key, e._Emaps_key
		into temporary table gxd_structure_map
		from ad a, emaps e
		where a._mapping_key = e._mapping_key'''	

	cmd1 = 'create index ad1 on gxd_structure_map (_Structure_key)'
	cmd2 = 'create index emaps1 on gxd_structure_map (_Emaps_key)'

	for cmd in [ cmd0, cmd1, cmd2 ]:
		dbAgnostic.execute(cmd)
	logger.debug('Computed mapping table from AD to EMAPS')

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
				r._Result_key, rs._Structure_key
			from gxd_insituresult r, gxd_isresultstructure rs
			where r._Result_key = rs._Result_key
		)
		select s._Assay_key, s._Genotype_key, p.is_present,
			r._Result_key, r._Structure_key, s._Specimen_key
		into temporary table gxd_insitu_results
		from specimens s, results r, gxd_present p
		where s._Specimen_key = r._Specimen_key
		and r._Strength_key = p._Strength_key'''

	cmd7 = 'create index isr1 on gxd_insitu_results (_Structure_key)'
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
				gs._Structure_key
			from gxd_gellane g, gxd_gellanestructure gs,
				gxd_assays ga
			where ga._Assay_key = g._Assay_key
				and g._GelLane_key = gs._GelLane_key
				and g._GelControl_key = 1
			)
		select g._Assay_key, g._Genotype_key, g._GelLane_key,
			g._Structure_key, max(p.is_present) as is_present
		into temporary table gxd_gel_results
		from lanes g, gxd_gelband b, gxd_present p
		where g._GelLane_key = b._GelLane_key
			and b._Strength_key = p._Strength_key
		group by 1, 2, 3, 4'''

	cmd10 = 'create index gg1 on gxd_gel_results (_Structure_key)'
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
			e._Emaps_key, isr.is_present, 0
		from gxd_insitu_results isr, gxd_structure_map e,
			gxd_assay ga
		where isr._Structure_key = e._Structure_key
			and isr._Assay_key = ga._Assay_key''' % GXD_CACHE

	cmd14 = '''insert into %s
		select ga._Marker_key, ggr._Assay_key, ggr._Genotype_key,
			e._Emaps_key, ggr.is_present, 0
		from gxd_gel_results ggr, gxd_structure_map e,
			gxd_assay ga
		where ggr._Assay_key = ga._Assay_key
			and ggr._Structure_key = e._Structure_key''' % \
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
	cmd22 = 'drop table gxd_structure_map'
	cmd23 = 'drop table gxd_present'
	cmd24 = 'drop table gxd_gel_results'

	for cmd in [ cmd21, cmd22, cmd23, cmd24 ]:
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
	# Is: a HeadingCollection for the MP (Mammalian Phenotype) slimgrid,
	#	read from a data file

	def initialize (self):
		# input data file is a tab-delimited file with three columns:
		#	header term ID, header term, header abbreviation
		# mpFile is the full path to this file
		mpFile = None

		# verify that we have the file specified in the config
		try:
			mpFile = config.MP_SLIMGRID_HEADERS
		except:
			raise error, \
			'Missing configuration parameter MP_SLIMGRID_HEADERS'

		# verify that the path is valid
		if not os.path.exists(mpFile):
			raise error, 'Cannot find file: %s' % mpFile

		# read from the file and raise exception if we can't
		try:
			fp = open(mpFile, 'r')
			rawLines = fp.readlines()
			fp.close()
		except:
			raise error, 'Error reading from: %s' % mpFile

		# slimgrid definitions

		slimgrid = 'Mammalian Phenotype'
		slimgridAbbrev = 'MP'

		# remove any blank lines or commented lines from the list of
		# data lines

		lines = []
		for line in rawLines:
			line = line.strip()
			if (line != '') and (line[0] != '#'):
				lines.append(line.split('\t'))

		# at this point, lines is a list of lists.  Each sublist 
		# should have three items:  [ term ID, term, abbrev ].  We're
		# going to expand these to add the term key as a fourth column.

		lineNum = 0
		for line in lines:
			lineNum = lineNum + 1

			if len(line) < 3:
				raise error, 'In %s line %d has too few fields' % (mpFile, lineNum)
			if len(line) > 3:
				raise error, 'In %s line %d has too many fields' % (mpFile, lineNum)

			termKey = VocabUtils.getKey(line[0])
			if not termKey:
				raise error, 'In %s line %s, cannot find term key for ID %s' % (mpFile, lineNum, line[0])

			line.append(termKey)
	
		# now store the data from the data file internally
		for [ termID, term, abbrev, termKey ] in lines:
			self.store(getNextHeadingKey(), term, abbrev, termKey,
				termID, slimgrid, slimgridAbbrev,
				getNextSequenceNum())

		logger.debug('Got %d MP headers' % len(lines))
		return

class MPHeadingCollectionOld (HeadingCollection):
	# Is: a HeadingCollection for the MP (Mammalian Phenotype) slimgrid

	def initialize (self):
		cmd = '''select distinct t.term,
				t._Term_key,
				a.accID,
				t.sequenceNum
			from %s vah,
				voc_term t,
				acc_accession a
			where vah._Term_key = t._Term_key
				and t._Term_key = a._Object_key
				and a.private = 0
				and a.preferred = 1
				and a._MGIType_key = 13
			order by t.sequenceNum''' % MP_HEADER_KEYS

		(cols, rows) = dbAgnostic.execute(cmd)

		termCol = dbAgnostic.columnNumber (cols, 'term')
		keyCol = dbAgnostic.columnNumber (cols, '_Term_key')
		idCol = dbAgnostic.columnNumber (cols, 'accID')

		slimgrid = 'Mammalian Phenotype'
		slimgridAbbrev = 'MP'

		for row in rows:
			accID = row[idCol]
			termKey = row[keyCol]
			term = row[termCol].strip()
			termAbbrev = term.replace(' phenotype', '')

			self.store(getNextHeadingKey(), term, termAbbrev,
				termKey, accID,
				slimgrid, slimgridAbbrev, getNextSequenceNum())

		logger.debug('Got %d MP headers' % len(rows))
		return

class GOHeadingCollection (HeadingCollection):
	# Is: a HeadingCollection for the GO (Gene Ontology) slimgrids

	def initialize (self):
		cmd = '''select distinct t.abbreviation as term,
				t._Term_key,
				a.accID,
				t.sequenceNum,
				dd.name as ontology,
				dd.abbreviation
			from %s ghk,
				voc_term t,
				acc_accession a,
				dag_node dn,
				dag_dag dd
			where ghk._Term_key = t._Term_key
				and t._Term_key = a._Object_key
				and a.private = 0
				and a.preferred = 1
				and a._MGIType_key = 13
				and t._Term_key = dn._Object_key
				and dn._DAG_key = dd._DAG_key
				and dd._MGIType_key = 13
			order by t.sequenceNum''' % GO_HEADER_KEYS

		(cols, rows) = dbAgnostic.execute(cmd)

		termCol = dbAgnostic.columnNumber (cols, 'term')
		keyCol = dbAgnostic.columnNumber (cols, '_Term_key')
		idCol = dbAgnostic.columnNumber (cols, 'accID')
		ontologyCol = dbAgnostic.columnNumber (cols, 'ontology')
		abbrevCol = dbAgnostic.columnNumber (cols, 'abbreviation')

		headingKeys = {}	# maps from heading to (heading key,
					# sequence num)

		for row in rows:
			accID = row[idCol]
			termKey = row[keyCol]
			term = row[termCol].strip()
			termAbbrev = term
			slimgrid = row[ontologyCol]
			slimgridAbbrev = row[abbrevCol]

			if not headingKeys.has_key(term):
				headingKeys[term] = (getNextHeadingKey(),
					getNextSequenceNum())

			headingKey, seqNum = headingKeys[term]

			self.store(headingKey, term, termAbbrev, termKey,
				accID, slimgrid, slimgridAbbrev, seqNum)

		logger.debug('Got %d terms for %d GO headers' % (len(rows),
			len(headingKeys)))
		return

class GxdHeadingCollection (HeadingCollection):
	# Is: a HeadingCollection for the GXD anatomy slimgrid

	def initialize (self):
		cmd = '''select distinct t.abbreviation as term,
				t._Term_key,
				a.accID,
				t.sequenceNum
			from %s vah,
				voc_term t,
				acc_accession a
			where vah._Term_key = t._Term_key
				and t._Term_key = a._Object_key
				and a.private = 0
				and a.preferred = 1
				and a._MGIType_key = 13
			order by t.sequenceNum''' % GXD_HEADER_KEYS

		(cols, rows) = dbAgnostic.execute(cmd)

		termCol = dbAgnostic.columnNumber (cols, 'term')
		keyCol = dbAgnostic.columnNumber (cols, '_Term_key')
		idCol = dbAgnostic.columnNumber (cols, 'accID')

		slimgrid = 'Anatomy'
		slimgridAbbrev = 'Anatomy'

		for row in rows:
			accID = row[idCol]
			termKey = row[keyCol]
			term = row[termCol].strip()
			termAbbrev = term

			self.store(getNextHeadingKey(), term, termAbbrev,
				termKey, accID,
				slimgrid, slimgridAbbrev, getNextSequenceNum())

		logger.debug('Got %d anatomy headers' % len(rows))
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
		#setupPhenoHeaderKeys()
		self.setMPHeaderCollection(MPHeadingCollection())
		
		# custom anatomy setup
		setupGxdHeaderKeys()
		setupGxdCacheTables()
		self.setGxdHeaderCollection(GxdHeadingCollection())
		

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
		setupPhenoHeaderKeys(mph)
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

		markerCol = dbAgnostic.columnNumber (cols, '_Marker_key')
		headerCol = dbAgnostic.columnNumber (cols, '_Term_key')
		countCol = dbAgnostic.columnNumber (cols, 'annot_count')

		byMarker = {}	# marker key -> term key -> count

		# collect the non-zero counts from the database

		for row in rows:

			# NEED TO TRANSLATE FROM HEADER TERM KEY TO HEADER KEY

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
		from mrk_marker
		where _Marker_key >= %d
			and _Marker_key < %d
			and _Organism_key = 1
			and _Marker_Status_key in (1,3)
	),
	annotations as (
		select va._Object_key, mhk._Term_key, vep.value as _Annot_key,
			va._Term_key as annotated_term_key
		from voc_annot va, dag_closure dc, mp_header_keys mhk,
			markers m, voc_evidence ve, voc_evidence_property vep,
			voc_term vp
		where va._AnnotType_key = 1015
			and va._Object_key = m._Marker_key
			and va._Term_key = dc._DescendentObject_key
			and dc._AncestorObject_key = mhk._Term_key
			and va._Qualifier_key != 2181424
			and va._Annot_key = ve._Annot_key
			and ve._AnnotEvidence_key = vep._AnnotEvidence_key
			and vep._PropertyTerm_key = vp._Term_key
			and vp.term = '_SourceAnnot_key' 
	)
	select _Object_key, _Term_key,
		count(distinct annotated_term_key) as annot_count
	from annotations
	group by _Object_key, _Term_key''',

	# 1. A unique GO annotation for a marker is a unique set of (marker
	# key, term, qualifier, evidence code, and inferred-from value).  When
	# we count these, we must also exclude any unwanted No Data annotations
	# (those annotations with an ND evidence code, where the annotated
	# marker has another annotation within the same DAG).  Need to get 
	# annotations to ancestors via the DAG, plus annotations directly to
	# the header terms themselves.
	'''
	with markers as (
		select _Marker_key
		from mrk_marker
		where _Marker_key >= %d
			and _Marker_key < %d
			and _Organism_key = 1
			and _Marker_Status_key in (1,3)
	),
	annotations as (
		select distinct gh._Term_key, k._Marker_key, k._DAG_key,
			k._Qualifier_key, k._EvidenceTerm_key, k.inferredFrom,
			k._Term_key as annotated_term_key
		from go_header_keys gh, go_closure dc, keepers k, markers m
		where gh._Term_key = dc._AncestorObject_key
		and dc._DescendentObject_key = k._Term_key
		and k._Marker_key = m._Marker_key
	)
	select _Marker_key, _Term_key, count(1) as annot_count
	from annotations
	group by _Marker_key, _Term_key''',	

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
		from mrk_marker
		where _Marker_key >= %d
			and _Marker_key < %d
			and _Organism_key = 1
			and _Marker_Status_key in (1,3)
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
	group by _Marker_key, _Term_key''',
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

# global instance of a SlimgridGatherer
gatherer = SlimgridGatherer (files, cmds)
gatherer.setupChunking (
	'select min(_Marker_key) from MRK_Marker where _Organism_key = 1',
	'select max(_Marker_key) from MRK_Marker where _Organism_key = 1',
	10000
	)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
