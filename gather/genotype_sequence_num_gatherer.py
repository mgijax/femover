#!/usr/local/bin/python
# 
# gathers data for the 'genotype_sequence_num' table in the front-end database

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

	def collateResults (self):
		# list of (allele symbol, allele key) to be sorted
		alleleList = []

		cols, rows = self.results[0]
		keyCol = Gatherer.columnNumber (cols, '_Allele_key')
		symbolCol = Gatherer.columnNumber (cols, 'symbol')

		for row in rows:
			alleleList.append ( (row[symbolCol].lower(),
				row[keyCol]) )

		alleleList.sort (lambda a, b : symbolsort.nomenCompare(a[0],
			b[0]) )

		alleleSortVal = {}	# allele key -> sequence num
		i = 0
		for (symbol, key) in alleleList:
			i = i + 1
			alleleSortVal[key] = i

		# sort null alleles first
		alleleSortVal[None] = 0

		logger.debug ('Sorted %d allele symbols' % len(alleleSortVal))

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
			genotypes[genotypeKey].append (
				alleleSortVal[allele1Key])

			genotypes[genotypeKey].append (
				alleleSortVal[allele2Key])

		genotypeList = genotypes.items()
		genotypeList.sort(lambda a, b : cmp(a[1], b[1]))

		logger.debug ('Sorted %d genotypes' % len(genotypeList))

		# prepare set of final results

		self.finalColumns = [ 'genotypeKey', 'byAlleles' ]
		self.finalResults = []

		i = 0
		for (key, alleleList) in genotypeList:
			i = i + 1
			self.finalResults.append ( [ key, i ] )
		return

###--- globals ---###

cmds = [
	# get symbols for all alleles which participate in genotypes, so we
	# can sort them
	'''select a._Allele_key, a.symbol
	from all_allele a, gxd_allelepair g
	where a._Allele_key = g._Allele_key_1
	union
	select a._Allele_key, a.symbol
	from all_allele a, gxd_allelepair g
	where a._Allele_key = g._Allele_key_2''',

	# get all allele pairs for all genotypes
	'''select _Genotype_key, _Allele_key_1, _Allele_key_2, sequenceNum
	from gxd_allelepair
	order by _Genotype_key, sequenceNum'''
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ 'genotypeKey', 'byAlleles', ]

# prefix for the filename of the output file
filenamePrefix = 'genotype_sequence_num'

# global instance of a GenotypeSequenceNumGatherer
gatherer = GenotypeSequenceNumGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
