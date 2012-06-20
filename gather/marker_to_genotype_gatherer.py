#!/usr/local/bin/python
# 
# gathers data for the 'marker_to_genotype' table in the front-end database

import Gatherer

###--- Classes ---###

MarkerToGenotypeGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the marker_to_genotype table
	# Has: queries to execute against the source database
	# Does: queries the source database for associations among markers and
	#	the genotypes which contain mutated alleles of those markers,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	'''select distinct _Marker_key,
		_Genotype_key,
		null as _Refs_key,
		null as qualifier
	from gxd_allelegenotype'''
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', '_Genotype_key', '_Refs_key',
	'qualifier'
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_to_genotype'

# global instance of a MarkerToGenotypeGatherer
gatherer = MarkerToGenotypeGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
