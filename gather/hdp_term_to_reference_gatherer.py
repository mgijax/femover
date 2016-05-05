#!/usr/local/bin/python
# 
# gathers data for the 'hdp_term_to_reference' table in the front-end database

import Gatherer
import logger
import DiseasePortalUtils

###--- Classes ---###

class TermToReferenceGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the hdp_term_to_reference table
	# Has: queries to execute against the source database
	# Does: queries the source database for data about terms and references
	#	for the HMDC, collates results, writes tab-delimited text file

	def collateResults (self):
		self.finalColumns = [ '_Term_key', '_Refs_key' ]
		self.finalResults = []

		termToRefs = DiseasePortalUtils.getReferencesByDiseaseKey()
		termKeys = termToRefs.keys()
		termKeys.sort()

		for termKey in termKeys:
			refsKeys = termToRefs[termKey]
			refsKeys.sort()

			for refsKey in refsKeys:
				self.finalResults.append( [termKey, refsKey] )

		logger.debug('Collected %d term/reference pairs' % \
			len(self.finalResults))
		return 


###--- globals ---###

cmds = [
	# 0. all data retrieval is in a library, so just have a token SQL
	# command to make the Gatherer happy (exception if no SQL commands)
	'select 1'
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Term_key', '_Refs_key' ]

# prefix for the filename of the output file
filenamePrefix = 'hdp_term_to_reference'

# global instance of a TermToReferenceGatherer
gatherer = TermToReferenceGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
