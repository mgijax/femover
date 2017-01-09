#!/usr/local/bin/python
# 
# This gatherer creates all the extra tables needed for the allele summary 
#
# Author: kstone - dec 2013
#

import Gatherer
import dbAgnostic
import logger

###--- Constants ---###
MP_ANNOTTYPE_KEY=1002
OMIM_ANNOTTYPE_KEY=1005
NORMAL_PHENOTYPE="no abnormal phenotype observed"
NOT_ANALYZED="not analyzed"

NO_PHENOTYPIC_ANALYSIS = 293594		# key for 'no phenotypic analysis'
EXPRESSES_COMPONENT = 1004		# category key for EC relationships
ALLELE_SUBTYPE = 1014			# annot type key for allele subtypes
RECOMBINASE = 11025588			# term key for recombinase subtype

# name of (allele key, genotype key) temp table
TEMP_TABLE = 'tmp_allele_genotype'

###---- Functions ---###
def getCount(table, whereClause=''):
	if whereClause:
		where = ' where %s' % whereClause
	else:
		where = ''

	cols, rows = dbAgnostic.execute('select count(1) from %s%s' % (
		table, where))
	return rows[0][0]

def createTempTables():
	# This function creates a temp table (named by TEMP_TABLE) which maps
	# from an allele key to the genotype keys for which headers should be
	# displayed on the allele summary page.  Currently, this set of
	# genotypes should include:
	#    1. naturally simple, one-marker genotypes (one allele pair)
	#    2. conditional genotypes which leave only one marker when you
	#	remove the recombinase alleles which do not have expressed
	#	component relationships.  (also remove wild-type alleles)

	# temp tables used in this function
	gpc = 'genotype_pair_counts'
	gam = 'genotype_allele_marker'
	gmc = 'genotype_marker_counts'

	# 1. build temp table gpc, mapping from a genotype key to its count
	# of allele pairs.  Includes only genotypes with OMIM/MP annotations
	# and to terms other than 'no phenotypic analysis'.

	q1 = '''select p._Genotype_key, count(1) as pair_count
		into temp table %s
		from gxd_allelepair p
		where exists (select 1 from voc_annot v
			where v._AnnotType_key in (%d,%d)
			and v._Term_key != %d
			and v._Object_key = p._Genotype_key)
		group by p._Genotype_key''' % (gpc, MP_ANNOTTYPE_KEY,
			OMIM_ANNOTTYPE_KEY, NO_PHENOTYPIC_ANALYSIS)
	dbAgnostic.execute(q1)

	q1index = 'create index q1 on %s(_Genotype_key)' % gpc
	dbAgnostic.execute(q1index)

	postQ1 = getCount(gpc)
	logger.debug('Put %d allele pair counts in %s' % (postQ1, gpc))

	# 2. collect one-marker genotypes into TEMP_TABLE

	q2 = '''select distinct gpc._Genotype_key, gag._Allele_key
		into temp table %s
		from %s gpc,
			gxd_allelegenotype gag
		where gpc._Genotype_key = gag._Genotype_key
			and gpc.pair_count < 2''' % (TEMP_TABLE, gpc)
	dbAgnostic.execute(q2)

	q2index1 = 'create index q2a on %s(_Genotype_key)' % TEMP_TABLE
	q2index2 = 'create index q2b on %s(_Allele_key)' % TEMP_TABLE
	dbAgnostic.execute(q2index1)
	dbAgnostic.execute(q2index2)

	postQ2 = getCount(TEMP_TABLE)
	logger.debug('Added %d one-marker genotypes to %s' % (
		postQ2, TEMP_TABLE))

	# 3. We now need to look at genotypes that weren't added to TEMP_TABLE
	# already, to see if we can narrow down some to a single marker by
	# removing consideration of wild-type alleles and recombinase alleles
	# which do not have expressed component relationships.

	q3 = '''select distinct gag._Genotype_key, 
			gag._Allele_key, 
			gag._Marker_key,
			gg.isConditional,
			0 as hasEC,
			0 as isRecombinase
		into temp table %s
		from gxd_allelegenotype gag,
			all_allele aa,
			gxd_genotype gg,
			%s gpc
		where gag._Genotype_key = gpc._Genotype_key
			and gag._Genotype_key = gg._Genotype_key
			and gag._Allele_key = aa._Allele_key
			and aa.isWildType = 0
			and gpc.pair_count > 1''' % (gam, gpc)
	dbAgnostic.execute(q3)
	q3index1 = 'create index q3a on %s(_Genotype_key)' % gam
	q3index2 = 'create index q3b on %s(_Allele_key)' % gam
	q3index3 = 'create index q3c on %s(_Marker_key)' % gam

	for q3i in [ q3index1, q3index2, q3index3 ]:
		dbAgnostic.execute(q3i)

	postQ3 = getCount(gam)
	logger.debug('Added %d rows to %s' % (postQ3, gam))

	q4 = '''update %s
		set hasEC = 1
		where _Allele_key in (select _Object_key_1
			from mgi_relationship
			where _Category_key = %d)''' % (
				gam, EXPRESSES_COMPONENT)
	dbAgnostic.execute(q4)
	logger.debug('Identified %d rows with EC relationships' % \
		getCount(gam, 'hasEC = 1'))

	q5 = '''update %s
		set isRecombinase = 1
		where _Allele_key in (select _Object_key
			from voc_annot
			where _AnnotType_key = %d
				and _Term_key = %d)''' % (
					gam, ALLELE_SUBTYPE, RECOMBINASE)
	dbAgnostic.execute(q5)
	logger.debug('Identified %d rows with recombinase alleles' % \
		getCount(gam, 'isRecombinase = 1'))

	q6 = '''delete from %s
		where isRecombinase = 1
			and hasEC = 0''' % gam
	dbAgnostic.execute(q6)

	postQ6 = getCount(gam)
	logger.debug('Removed %d rows from %s' % (postQ3 - postQ6, gam))

	q7 = '''select _Genotype_key,
			count(distinct _Marker_key) as marker_count
		into temp table %s
		from %s
		group by _Genotype_key''' % (gmc,gam)
	dbAgnostic.execute(q7)

	logger.debug('Added %d rows to %s' % (getCount(gmc), gmc))

	q8 = '''insert into %s
		select distinct gmc._Genotype_key, gam._Allele_key
		from %s gmc,
			%s gam
		where gmc._Genotype_key = gam._Genotype_key
			and gmc.marker_count = 1''' % (TEMP_TABLE, gmc, gam)
	dbAgnostic.execute(q8)

	postQ8 = getCount(TEMP_TABLE)
	logger.debug('Added %d simplified genotypes to %s' % (
		postQ8 - postQ2, TEMP_TABLE))



	# select allele_key->genotype_key mappings where allelepairnum<2 OR genotype is conditional
#        tempTable1="""
#		select gag._allele_key,gag._genotype_key
#		into temp table %s
#                from gxd_AlleleGenotype gag,
#		gxd_genotype gg,
#		(select _genotype_key,max(sequencenum) allelepairnum
#		   from gxd_allelepair gap group by _genotype_key) gap1
#                where gag._genotype_key=gap1._genotype_key
#			and gg._genotype_key=gag._genotype_key
#			and (gap1.allelepairnum<2 or gg.isconditional=1)
#        """ % TEMP_TABLE

        tempIdx1="""
                create index idx_snp_mrk_key on %s (_genotype_key)
        """ % TEMP_TABLE
#        logger.debug("creating temp table for alleles to simple genotypes")
#        dbAgnostic.execute(tempTable1)
        logger.debug("indexing temp table for alleles to simple genotypes")
        dbAgnostic.execute(tempIdx1)
        logger.debug("done creating temp tables")

###--- Classes ---###
class AlleleSummaryGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for mp_annotation table 
	# Has: queries to execute against the source database

	###--- Program Flow ---###
	def buildSummarySystems(self):
		cols=["allele_system_key","allele_key","system"]
		rows=[]
		# collect all the abnormal allele keys for future lookup
		allAbnorm=set([])
		for r in self.results[3][1]:
			allAbnorm.add(r[0])

		uniqueRows=set([])
		allSysMap={}
		
		for r in self.results[0][1]:
			allKey=r[0]
			isNorm=r[1]
			header=r[2]

			# separate not analyzed from the other normals, so we can differentiate later on
			if header==NOT_ANALYZED:
				isNorm=0

			# test uniqueness
			uniqueId=(allKey,header,isNorm)
			if uniqueId in uniqueRows:
				continue
			uniqueRows.add(uniqueId)

			# init header map to prove that normals exist for this allele
			# it will be overidden if there are any abnormals
			if isNorm:
				allSysMap.setdefault(allKey,[])
				continue

			# map abnormal headers to allele keys (We don't get here if isNorm == 1)
			allSysMap.setdefault(allKey,[]).append(header)

		uniqueKey=0
		for allKey,headers in allSysMap.items():
			if not headers or headers==[NORMAL_PHENOTYPE]: 
				if allKey in allAbnorm:
					# ignore if there is abnormal data in complex genotypes
					continue
				# if all we have are normal headers, use this custom text
				uniqueKey+=1
				rows.append((uniqueKey,allKey,"no abnormal phenotype observed"))
			elif headers==[NOT_ANALYZED]:
				if allKey in allAbnorm:
					# ignore if there is abnormal data in complex genotypes
					continue
				uniqueKey+=1
				rows.append((uniqueKey,allKey,"no phenotypic analysis"))
			else:
				for header in headers:
					if header not in [NOT_ANALYZED,NORMAL_PHENOTYPE]:
						uniqueKey+=1
						rows.append((uniqueKey,allKey,header))
		return cols,rows

	def buildSummaryDiseases(self):
		cols=["allele_disease_key","allele_key","disease","omim_id"]
		rows=[]
		uniqueKey=0
		uniqueRows=set([])
		for r in self.results[1][1]:
			allKey=r[0]
			disease=r[1]
			omimId=r[2]

			# test uniqueness
			uniqueId=(allKey,omimId)
			if uniqueId in uniqueRows:
				continue
			uniqueRows.add(uniqueId)

			uniqueKey+=1
			rows.append((uniqueKey,allKey,disease,omimId))
		return cols,rows

	def buildSummaryGenotypes(self):
		cols=["allele_genotype_key","allele_key","genotype_key"]
		rows=[]
		uniqueKey=0
		uniqueRows=set([])
		for r in self.results[2][1]:
			uniqueKey+=1
			rows.append((uniqueKey,r[0],r[1]))
		return cols,rows

	# here is defined the high level script for building all these tables
	# by gatherer convention we create tuples of (cols,rows) for every table we want to create, and append them to self.output
	def buildRows (self):
		logger.debug("building summary system rows")
		systemCols,systemRows=self.buildSummarySystems()

		logger.debug("building summary disease rows")
		diseaseCols,diseaseRows=self.buildSummaryDiseases()

		logger.debug("building summary genotype rows")
		genoCols,genoRows=self.buildSummaryGenotypes()

		logger.debug("done. outputing to file")
		self.output=[(systemCols,systemRows),(diseaseCols,diseaseRows),(genoCols,genoRows)]

	# this is a function that gets called for every gatherer
	def collateResults (self):
		# process all queries
		self.buildRows()

###--- MGD Query Definitions---###
# all of these queries get processed before collateResults() gets called
cmds = [
	# 0. get all systems to allele_keys
	'''
	select tag._allele_key,
		vah.isNormal,
		ms.synonym as header,
		vah._term_key
	from %s tag,
		VOC_AnnotHeader vah,
		VOC_Term vt,
		MGI_Synonym ms,
		MGI_SynonymType mst
	where tag._Genotype_key = vah._Object_key
		AND vah._Term_key = vt._Term_key
		AND vah._AnnotType_key = %d
		AND vt._Term_key = ms._Object_key
		AND ms._SynonymType_key = mst._SynonymType_key
		AND mst._MGIType_key = 13 
		AND mst.synonymType = 'Synonym Type 2'
	'''% (TEMP_TABLE, MP_ANNOTTYPE_KEY),
	# 1. get all diseases to allele_keys
	'''
	select tag._allele_key,
		vt.term,
		aa.accID omimId,
		vt._term_key
	from %s tag,
		VOC_Annot va,
		VOC_Term vt,
		ACC_Accession aa,
		VOC_Term vq
	where tag._Genotype_key = va._Object_key
		AND va._Qualifier_key = vq._Term_key
		AND vq.term IS null
		AND va._Term_key = vt._Term_key
		AND va._AnnotType_key = %d
		AND vt._Term_key = aa._Object_key
		AND aa._MGIType_key = 13 
		AND aa.preferred = 1
		AND aa.private = 0
	'''% (TEMP_TABLE, OMIM_ANNOTTYPE_KEY),
	# 2. allele summary genotypes
	'''
	select _allele_key,_genotype_key
	from %s tag
	''' % TEMP_TABLE,
	# 3. all alleles with an abnormal phenotype in a system
	'''
	select a._allele_key
	from all_allele a
	where exists (select 1 from gxd_allelegenotype gag,
			voc_annotheader vah,
			voc_term vt
		where gag._genotype_key=vah._object_key
			and vah._term_key=vt._term_key
			and vah._annottype_key= %d
			and vah.isnormal=0
			and vt.term!='normal phenotype'
			and gag._allele_key=a._allele_key)
	'''%MP_ANNOTTYPE_KEY
	]

###--- Table Definitions ---###
# definition of output files, each as:
#	(filename prefix, list of fieldnames, table name)
files = [
	('allele_summary_system',
		['allele_system_key','allele_key','system'],
		'allele_summary_system'),
	('allele_summary_disease',
		['allele_disease_key','allele_key','disease','omim_id'],
		'allele_summary_disease'),
	('allele_summary_genotype',
		['allele_genotype_key','allele_key','genotype_key'],
		'allele_summary_genotype'),
	]

createTempTables()
# global instance of a AnnotationGatherer
gatherer = AlleleSummaryGatherer (files, cmds)

###--- main program ---###
# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)

