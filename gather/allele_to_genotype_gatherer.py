#!/usr/local/bin/python
# 
# gathers data for the 'allele_to_genotype' table in the front-end database

import Gatherer
import GenotypeClassifier
import logger

###--- Classes ---###

class AlleleToGenotypeGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the allele_to_genotype table
	# Has: queries to execute against the source database
	# Does: queries the source database for associations between alleles
	#	and genotypes, collates results, writes tab-delimited file

	def collateResults (self):
		cols, rows = self.results[0]

		self.finalColumns = [ 'alleleGenotypeKey', '_Allele_key',
			'_Genotype_key', 'qualifier', 'genotype_type',
			'genotype_designation', 'hasPhenoData',
			'isDiseaseModel', 'sequenceNum' ]
		self.finalResults = []

		genotypeKeyCol = Gatherer.columnNumber (cols, '_Genotype_key')
		alleleKeyCol = Gatherer.columnNumber (cols, '_Allele_key')
		qualifierCol = Gatherer.columnNumber (cols, 'qualifier')

		for row in rows:
			# fields from the query

			genotype = row[genotypeKeyCol]
			allele = row[alleleKeyCol]
			qualifier = row[qualifierCol]

			# computed fields based on genotype/allele keys

			alleleGenotypeKey = \
				GenotypeClassifier.getAlleleGenotypeKey (
					allele, genotype)
			seqNumByAllele = GenotypeClassifier.getSequenceNum (
				allele, genotype)
			designation = GenotypeClassifier.getDesignation (
				allele, genotype)
			abbrev = GenotypeClassifier.getClass (genotype)

			# build the output row

			self.finalResults.append ( [ alleleGenotypeKey,
				allele, genotype, qualifier, abbrev,
				designation,
				GenotypeClassifier.hasPhenoData(genotype),
				GenotypeClassifier.isDiseaseModel(genotype),
				seqNumByAllele ] )

		logger.debug ('Gathered %d rows for allele_to_genotype' % \
			len(self.finalResults))
		return

###--- globals ---###

cmds = [
	'''select distinct _Genotype_key,
			_Allele_key,
			null as qualifier,
			sequenceNum
		from gxd_allelegenotype
		order by _Allele_key, sequenceNum'''
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ 'alleleGenotypeKey', '_Allele_key', '_Genotype_key',
	'qualifier', 'genotype_type', 'genotype_designation',
	'hasPhenoData', 'isDiseaseModel', 'sequenceNum' ]

# prefix for the filename of the output file
filenamePrefix = 'allele_to_genotype'

# global instance of a AlleleToGenotypeGatherer
gatherer = AlleleToGenotypeGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
