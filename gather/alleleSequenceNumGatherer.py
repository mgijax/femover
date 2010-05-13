#!/usr/local/bin/python
# 
# gathers data for the 'alleleSequenceNum' table in the front-end database

import Gatherer
import logger
import symbolsort

###--- Classes ---###

class AlleleSequenceNumGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the alleleSequenceNum table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for alleles,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single allele,
		#	rather than for all alleles

		if self.keyField == 'alleleKey':
			return 'm._Allele_key = %s' % self.keyValue
		return ''

	def collateResults (self):
		dict = {}

		# allele symbol
		items = []
		for row in self.results[0]:
			alleleKey = row['_Allele_key']
			d = { '_Allele_key' : alleleKey,
				'bySymbol' : 0,
				'byAlleleType' : 0,
				'byPrimaryID' : 0 }
			dict[alleleKey] = d

			items.append ( (row['symbol'].lower(), alleleKey) )

		items.sort (lambda a, b : symbolsort.nomenCompare(a[0], b[0]))
		i = 1
		for (symbol, alleleKey) in items:
			dict[alleleKey]['bySymbol'] = i
			i = i + 1
		logger.debug ('Collated symbol data')

		# allele type
		items = []
		for row in self.results[1]:
			items.append ((row['term'].lower(), row['_Term_key']))
		items.sort()

		byTypeKey = {}
		i = 1
		for (name, key) in items:
			byTypeKey[key] = i
			i = i + 1

		for row in self.results[2]:
			alleleKey = row['_Allele_key']
			dict[alleleKey]['byAlleleType'] = \
				byTypeKey[row['_Allele_Type_key']]
		logger.debug ('Collated type data')

		# primary ID
		items = []
		for row in self.results[3]:
			items.append ( (row['prefixPart'], row['numericPart'],
				row['_Allele_key']) )
		items.sort()

		i = 1
		for (prefixPart, numericPart, alleleKey) in items:
			dict[alleleKey]['byPrimaryID'] = i
			i = i + 1
		logger.debug ('Collated primary IDs')

		self.finalResults = dict.values() 
		return

###--- globals ---###

cmds = [
	'select m._Allele_key, m.symbol from ALL_Allele m %s',

	'''select t._Term_key, t.term
		from VOC_Term t, VOC_Vocab v
		where t._Vocab_key = v._Vocab_key
			and v.name = "Allele Type" ''',

	'select m._Allele_key, m._Allele_Type_key from ALL_Allele m %s',

	'''select m._Allele_key, a.prefixPart, a.numericPart
		from ACC_Accession a, ACC_LogicalDB ldb, ALL_Allele m
		where a._MGIType_key = 11
			and a._LogicalDB_key = ldb._LogicalDB_key
			and a.preferred = 1
			and m._Allele_key = a._Object_key %s''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	'_Allele_key', 'bySymbol', 'byAlleleType', 'byPrimaryID',
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
