#!/usr/local/bin/python
# 
# gathers data for the 'alleleSequenceNum' table in the front-end database

import Gatherer
import logger
import symbolsort

###--- Globals ---###

SYMBOL = 'alleleSymbol'
TYPE = 'alleleType'
ID = 'alleleID'

###--- Classes ---###

class AlleleSequenceNumGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the alleleSequenceNum table
	# Has: queries to execute against the source database
	# Does: queries the source database for ordering data for alleles,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		# dict[allele key] = [ allele key, sort 1, sort 2, ... ]
		# (assumes the sort items in the list are in order to match
		# 'fieldOrder')
		dict = {}

		counts = [] 		# ordered names for counts

		# allele symbol
		# assumes that all alleles will be returned in query 0

		symbols = []	# list of items to sort: (symbol, allele key)

		keyCol = Gatherer.columnNumber (self.results[0][0],
			'_Allele_key')
		symCol = Gatherer.columnNumber (self.results[0][0], 'symbol')

		for row in self.results[0][1]:
			alleleKey = row[keyCol]

			# build list of symbols to sort
			symbols.append ( (row[symCol].lower(), alleleKey) )

		symbols.sort (lambda a, b : symbolsort.nomenCompare(a[0],
			b[0]))
		counts.append (SYMBOL)
		i = 1
		for (symbol, alleleKey) in symbols:
			dict[alleleKey] = [ alleleKey, i ]
			i = i + 1
		logger.debug ('Collated symbol data')

		# allele type
		# assumes that all alleles will be returned in query 1
		keyCol = Gatherer.columnNumber (self.results[1][0],
			'_Allele_key')
		sortCol = Gatherer.columnNumber (self.results[1][0],
			'sequenceNum')
		counts.append (TYPE)

		for row in self.results[1][1]:
			dict[row[keyCol]].append (row[sortCol])
		logger.debug ('Collated type data')

		# primary ID

		keyCol = Gatherer.columnNumber (self.results[2][0],
			'_Allele_key')
		preCol = Gatherer.columnNumber (self.results[2][0],
			'prefixPart')
		numCol = Gatherer.columnNumber (self.results[2][0],
			'numericPart')

		counts.append (ID)

		ids = []
		for row in self.results[2][1]:
			ids.append ( (row[preCol], row[numCol], row[keyCol]) )
		ids.sort()

		allKeys = {}
		for key in dict.keys():
			allKeys[key] = 1

		i = 1
		for (prefixPart, numericPart, alleleKey) in ids:
			# if we've already seen an earlier ID for this allele,
			# then skip it
			if allKeys.has_key (alleleKey):
				dict[alleleKey].append (i)
				del allKeys[alleleKey]
				i = i + 1

		# handle alleles without IDs (if there are any)
		for key in allKeys.keys():
			dict[alleleKey].append (i)

		logger.debug ('Collated primary IDs')

		self.finalColumns = [ '_Allele_key' ] + counts
		self.finalResults = dict.values() 
		return

###--- globals ---###

cmds = [
	'select _Allele_key, symbol from all_allele',

	'''select a._Allele_key, t.sequenceNum
		from all_allele a, voc_term t
		where a._Allele_Type_key = t._Term_key''',

	'''select m._Allele_key, a.prefixPart, a.numericPart
		from acc_accession a, acc_logicalDB ldb, all_allele m
		where a._MGIType_key = 11
			and a._LogicalDB_key = ldb._LogicalDB_key
			and a.preferred = 1
			and m._Allele_key = a._Object_key''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	'_Allele_key', SYMBOL, TYPE, ID,
	]

# prefix for the filename of the output file
filenamePrefix = 'alleleSequenceNum'

# global instance of a AlleleSequenceNumGatherer
gatherer = AlleleSequenceNumGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
