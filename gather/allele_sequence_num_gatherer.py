#!/usr/local/bin/python
# 
# gathers data for the 'alleleSequenceNum' table in the front-end database

import Gatherer
import logger
import symbolsort

###--- Globals ---###

SYMBOL = 'alleleSymbol'
TYPE = 'alleleType'
CHR = 'chromosome'
ID = 'alleleID'
DRIVER = 'driver'

###--- Functions ---###

def driverCompare (a, b):
	# assumes a and b both contain:
	#	(driver text, symbol sort integer, allele key)

	res = symbolsort.nomenCompare(a[0], b[0])
	if res == 0:
		return cmp(a[1], b[1])
	return res

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
		counts.append (TYPE)

		for row in self.results[1][1]:
			dict[row[0]].append(row[1])

		logger.debug ('Collated type data')

		# allele chromosome 
		# assumes that all alleles will be returned in query 4
		counts.append (CHR)

		chrSortOrder=set([])
		for row in self.results[4][1]:
			chrSortOrder.add(row[1])
		chrSortOrder=list(chrSortOrder)
		chrSortOrder.sort(symbolsort.nomenCompare)
		chrSortMap={}
		cnt=0
		for chr in chrSortOrder:
			chrSortMap[chr]=cnt
			cnt+=1

		for row in self.results[4][1]:
			dict[row[0]].append(chrSortMap[row[1]])
		logger.debug ('Collated chromosome data')

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
			dict[key].append (i)

		logger.debug ('Collated primary IDs')

		# driver (for recombinase alleles)

		keyCol = Gatherer.columnNumber (self.results[3][0],
			'_Allele_key')
		driverCol = Gatherer.columnNumber (self.results[3][0],
			'driverNote')

		counts.append (DRIVER)

		# when sorting by driver, we need a two-level sort:
		#	1. driver, alphanumerically
		#	2. allele symbol (when driver matches)

		byDriver = []
		for row in self.results[3][1]:
			alleleKey = row[keyCol]
			symbolOrder = dict[alleleKey][1]
			byDriver.append ( (row[driverCol], symbolOrder,
				alleleKey) )
		byDriver.sort(driverCompare)

		allKeys = {}
		for key in dict.keys():
			allKeys[key] = 1

		i = 1
		for (driver, symbolOrder, alleleKey) in byDriver:
			# if we've already seen an earlier driver for this
			# allele, then skip it

			if allKeys.has_key (alleleKey):
				dict[alleleKey].append (i)
				del allKeys[alleleKey]
				i = i + 1

		# handle alleles without drivers (many)
		for key in allKeys.keys():
			dict[key].append (i)

		self.finalColumns = [ '_Allele_key' ] + counts
		self.finalResults = dict.values() 
		logger.info(self.finalResults[0])
		return

###--- globals ---###

cmds = [
	#0
	'select _Allele_key, symbol from all_allele',

	#1
	'''select a._Allele_key, t.sequenceNum
		from all_allele a, voc_term t
		where a._Allele_Type_key = t._Term_key''',

	#2
	'''select m._Allele_key, a.prefixPart, a.numericPart
		from acc_accession a, acc_logicalDB ldb, all_allele m
		where a._MGIType_key = 11
			and a._LogicalDB_key = ldb._LogicalDB_key
			and a.preferred = 1
			and m._Allele_key = a._Object_key''',

	#3
	'''select distinct _Allele_key, driverNote from all_cre_cache''',

	#4
	'''
	select a._allele_key,
		(case when m.chromosome is null then 'ZZZ'
			else m.chromosome end) as chromosome
	from all_allele a left outer join
		mrk_marker m on m._marker_key=a._marker_key	
	''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	'_Allele_key', SYMBOL, TYPE, CHR, ID, DRIVER,
	]

# prefix for the filename of the output file
filenamePrefix = 'allele_sequence_num'

# global instance of a AlleleSequenceNumGatherer
gatherer = AlleleSequenceNumGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
