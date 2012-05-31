#!/usr/local/bin/python
# 
# gathers data for the 'allele_mutation' table in the front-end database

import Gatherer

###--- Classes ---###

AlleleMutationGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the allele_mutation table
	# Has: queries to execute against the source database
	# Does: queries the source database for mutations for alleles,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [ '''select m._Allele_key, t.term
		from all_allele_mutation m, voc_term t
		where m._Mutation_key = t._Term_key
			and exists (select 1 from all_allele a
				where m._Allele_key = a._Allele_key)'''
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Allele_key', 'term' ]

# prefix for the filename of the output file
filenamePrefix = 'allele_mutation'

# global instance of a AlleleMutationGatherer
gatherer = AlleleMutationGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
