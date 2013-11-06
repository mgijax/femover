#!/usr/local/bin/python
# 
# gathers data for the 'genotype_sequence_num' table in the front-end database
#
# 10/30/2013	lec
#	- TR11423/human disease portal (hdp)
#	- moved "byAlleles" collation/results into its own function
#	- added "byHDP" function
#

import Gatherer
import symbolsort
import logger

###--- Functions ---###

###--- Classes ---###

class GenotypeSequenceNumGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the genotype_sequence_num table
	# Has: queries to execute against the source database
	# Does: queries the source database for sorting data for genotypes,
	#	collates results, writes tab-delimited text file

	def byAlleles(self, alleleSortVal):

		# to sort a genotype by its alleles, we need the sequence num
		# for all of its alleles in order (allele pair 1, ... n)

		cols, rows = self.results[1]
		genotypeCol = Gatherer.columnNumber (cols, '_Genotype_key')
		allele1Col = Gatherer.columnNumber (cols, '_Allele_key_1')
		allele2Col = Gatherer.columnNumber (cols, '_Allele_key_2')

		# genotype key -> [ allele key 1, ... allele key n ]
		genotypes = {}

		for row in rows:
			genotypeKey = row[genotypeCol]
			allele1Key = row[allele1Col]
			allele2Key = row[allele2Col]

			if not genotypes.has_key(genotypeKey):
				genotypes[genotypeKey] = []

			if not alleleSortVal.has_key(allele1Key):
				logger.debug ('No %s' % str(row))

			genotypes[genotypeKey].append (alleleSortVal[allele1Key])
			genotypes[genotypeKey].append (alleleSortVal[allele2Key])

		genotypeList = genotypes.items()
		genotypeList.sort(lambda a, b : cmp(a[1], b[1]))

		logger.debug ('Sorted %d genotypes by allele' % len(genotypeList))

		return genotypeList

	def byHDPRules(self, alleleSortVal):

		#
		# sql (3)
		# sort genotype by:  isConditional, pair state terms (see order below), allele symbol
		#

		cols, rows = self.results[3]
		genotypeCol = Gatherer.columnNumber (cols, '_Genotype_key')
		termCol = Gatherer.columnNumber (cols, 'term')
		conditionalCol = Gatherer.columnNumber (cols, 'isConditional')
		alleleKey1Col = Gatherer.columnNumber (cols, '_Allele_key_1')
		alleleKey2Col = Gatherer.columnNumber (cols, '_Allele_key_2')

		orderedHDP = []

		for row in rows:
			genotypeKey = row[genotypeCol]
			term = row[termCol]
			isConditional = row[conditionalCol]
			alleleKey1 = row[alleleKey1Col]
			alleleKey2 = row[alleleKey2Col]

			if term == 'Homozygous':
				s = 1
			elif term == 'Homoplasmic':
				s = 2
			elif term == 'Heterozygous':
				s = 3
			elif term == 'Heteroplasmic':
				s = 4
			elif term == 'Hemizygous X-linked':
				s = 5
			elif term == 'Hemizygous Y-linked':
				s = 6
			elif term == 'Hemizygous Insertion':
				s = 7
			elif term == 'Indeterminate':
				s = 8
			elif term == 'Hemizygous Deletion':
				s = 9

			# use the alleleSortVal-order
			alleleCount1 = alleleSortVal[alleleKey1]
			alleleCount2 = alleleSortVal[alleleKey2]

			orderedHDP.append((isConditional, s, alleleCount1, alleleCount2, genotypeKey))

		# order the list by term-specified order, isConditional, alleleCount
		orderedHDP.sort()

		# assign a unique by-hdp-number (i) to each genotype based on the orderedHDP list
		genotypeList = {}
		i = 1
		for o in orderedHDP:
			genotypeKey = o[3]
			if genotypeList.has_key(genotypeKey):
				continue
			genotypeList[genotypeKey] = []
			genotypeList[genotypeKey] = i
			i = i + 1
		#logger.debug (genotypeList)

		logger.debug ('Sorted %d genotypes by hdp-order' % len(genotypeList))

		return genotypeList

	def collateResults (self):

		# list of (allele symbol, allele key) to be sorted
		alleleList = []

		cols, rows = self.results[0]
		keyCol = Gatherer.columnNumber (cols, '_Allele_key')
		symbolCol = Gatherer.columnNumber (cols, 'symbol')

		for row in rows:
			alleleList.append ( (row[symbolCol].lower(), row[keyCol]) )

		alleleList.sort (lambda a, b : symbolsort.nomenCompare(a[0], b[0]) )

		alleleSortVal = {}	# allele key -> sequence num
		i = 0
		for (symbol, key) in alleleList:
			i = i + 1
			alleleSortVal[key] = i

		# sort null alleles first
		alleleSortVal[None] = 0

		logger.debug ('Sorted %d allele symbols' % len(alleleSortVal))

		# sql (1)
		genotypeByAlleleList = self.byAlleles(alleleSortVal)

		# sql (3)
		genotypeByHDPList = self.byHDPRules(alleleSortVal)

		# prepare set of final results

		self.finalColumns = [ 'genotypeKey', 'byAlleles', 'by_hdp_rules' ]
		self.finalResults = []

		i = 0
		for (key, alleleList) in genotypeByAlleleList:
			i = i + 1

			# find the hdp-order number for this genotype (if it exists)
			if genotypeByHDPList.has_key(key):
				byHDP = genotypeByHDPList[key]
			else:
				byHDP = 0

			self.finalResults.append ( [ key, i, byHDP ] )

		# now add those without alleles
		cols, rows = self.results[2]

		byHDP = 0	# always 0
		for row in rows:
			i = i + 1
			self.finalResults.append ( [ row[0], i, byHDP ] )

		return

###--- globals ---###

cmds = [
	# sql (0)
	# get symbols for all alleles which participate in genotypes, so we
	# can sort them
	'''select a._Allele_key, a.symbol
	from all_allele a, gxd_allelepair g
	where a._Allele_key = g._Allele_key_1
	union
	select a._Allele_key, a.symbol
	from all_allele a, gxd_allelepair g
	where a._Allele_key = g._Allele_key_2''',

	# sql (1)
	# get all allele pairs for all genotypes
	'''select _Genotype_key, _Allele_key_1, _Allele_key_2, sequenceNum
	from gxd_allelepair
	order by _Genotype_key, sequenceNum''',

	# sql (2)
	# get all genotypes which do not have allele pairs, so we can add
	# them, too
	'''select g._Genotype_key
	from gxd_genotype g
	where not exists (select 1 from gxd_allelepair p
		where g._Genotype_key = p._Genotype_key)
	order by 1''',

	#
	# sql (3)
	# get list of genotypes that contain MP/OMIM annotations ONLY
	#
	'''
	select distinct g._Genotype_key, t.term, g.isConditional, p._Allele_key_1, p._Allele_key_2
	from GXD_Genotype g, GXD_AllelePair p, VOC_Term t
	where g._Genotype_key = p._Genotype_key
	and p._PairState_key = t._Term_key
	and exists (select 1 from VOC_Annot v where v._AnnotType_key in (1002, 1005)
		and g._Genotype_key = v._Object_key)
	order by term desc, isConditional
	''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ 'genotypeKey', 'byAlleles', 'by_hdp_rules' ]

# prefix for the filename of the output file
filenamePrefix = 'genotype_sequence_num'

# global instance of a GenotypeSequenceNumGatherer
gatherer = GenotypeSequenceNumGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
