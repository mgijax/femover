#!/usr/local/bin/python
# 
# gathers data for the 'allele_to_genotype' table in the front-end database

import Gatherer

###--- Classes ---###

AlleleToGenotypeGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the allele_to_genotype table
	# Has: queries to execute against the source database
	# Does: queries the source database for associations between alleles
	#	and genotypes, collates results, writes tab-delimited file

###--- globals ---###

cmds = [
	'''select distinct _Genotype_key,
			_Allele_key,
			null as qualifier
		from gxd_allelegenotype'''
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Allele_key', '_Genotype_key', 'qualifier' ]

# prefix for the filename of the output file
filenamePrefix = 'allele_to_genotype'

# global instance of a AlleleToGenotypeGatherer
gatherer = AlleleToGenotypeGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
