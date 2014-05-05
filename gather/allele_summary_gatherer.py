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

###---- Functions ---###
def createTempTables():
	# select allele_key->genotype_key mappings where allelepairnum<2 OR genotype is conditional
        tempTable1="""
		select gag._allele_key,gag._genotype_key
		into temp table tmp_allele_genotype
                from gxd_AlleleGenotype gag,
		gxd_genotype gg,
		(select _genotype_key,max(sequencenum) allelepairnum
		   from gxd_allelepair gap group by _genotype_key) gap1
                where gag._genotype_key=gap1._genotype_key
			and gg._genotype_key=gag._genotype_key
			and (gap1.allelepairnum<2 or gg.isconditional=1)
        """
        tempIdx1="""
                create index idx_snp_mrk_key on tmp_allele_genotype (_genotype_key)
        """
        logger.debug("creating temp table for alleles to simple genotypes")
        dbAgnostic.execute(tempTable1)
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

			# map headers to allele keys
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
	from tmp_allele_genotype tag,
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
	'''%MP_ANNOTTYPE_KEY,
	# 1. get all diseases to allele_keys
	'''
	select tag._allele_key,
		vt.term,
		aa.accID omimId,
		vt._term_key
	from tmp_allele_genotype tag,
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
	'''%OMIM_ANNOTTYPE_KEY,
	# 2. allele summary genotypes
	'''
	select _allele_key,_genotype_key
	from tmp_allele_genotype tag
	''',
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

