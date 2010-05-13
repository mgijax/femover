#!/usr/local/bin/python
# 
# gathers data for the 'allele' table in the front-end database

import Gatherer
import sybaseUtil
import re

###--- Classes ---###

class AlleleGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the allele table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for alleles,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single allele,
		#	rather than for all alleles

		if self.keyField == 'alleleKey':
			return 'a._Allele_key = %s' % self.keyValue
		return ''

	def postprocessResults (self):
		symFinder = re.compile ('<([^>]*)>')
		for r in self.finalResults:
			r['logicalDB'] = sybaseUtil.resolve (
				r['_LogicalDB_key'], 'ACC_LogicalDB',
				'_LogicalDB_key', 'name')
			r['alleleType'] = sybaseUtil.resolve (
				r['_Allele_Type_key'])
			r['alleleSubType'] = None
			match = symFinder.search(r['symbol'])
			if match:
				r['onlyAlleleSymbol'] = match.group(1)
			else:
				r['onlyAlleleSymbol'] = r['symbol']
		return

###--- globals ---###

cmds = [
	'''select a._Allele_key, a.symbol, a.name, a._Allele_Type_key,
		ac.accID, ac._LogicalDB_key
	from ALL_Allele a, ACC_Accession ac
	where a._Allele_key = ac._Object_key
		and ac._MGIType_key = 11
		and ac.preferred = 1 
		and ac.private = 0 %s''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	'_Allele_key', 'symbol', 'name', 'onlyAlleleSymbol',
	'accID', 'logicalDB', 'alleleType', 'alleleSubType',
	]

# prefix for the filename of the output file
filenamePrefix = 'allele'

# global instance of a AlleleGatherer
gatherer = AlleleGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
