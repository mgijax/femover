#!/usr/local/bin/python
# 
# gathers data for the 'allele_recombinase_systems' table in the front-end
# database

import Gatherer
import logger

###--- Globals ---###


###--- Functions ---###

def prefix (s):
	if s:
		return s[:4].lower()
	return s

###--- Classes ---###

class AlleleRecombinaseSystemGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the allele_recombinase_systems table
	# Has: queries to execute against the source database
	# Does: queries the source database for allele counts,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		# Purpose: to combine the results of the various queries into
		#	one single list of final results, with one row per
		#	allele

		# results[allele key] = { system prefix : detected? }
		results = {}

		keyCol = Gatherer.columnNumber (self.results[0][0],
			'_Allele_key')
		systemCol = Gatherer.columnNumber (self.results[0][0],
			'cresystemlabel')
		expCol = Gatherer.columnNumber (self.results[0][0],
			'expressed')

		for row in self.results[0][1]:
			expressed = row[expCol]
			allele = row[keyCol]
			system = row[systemCol]

			if not results.has_key(allele):
				results[allele] = {}

			results[allele][system] = expressed

		logger.debug ('Processed %d rows' % len(self.results[0][1]))

		# at this point, 'results' is complete and we now need to work
		# to compose the final results rows

		self.finalResults = []

		for alleleKey, systemMap in results.items():
			detected = 0
			notDetected = 0

			for expressed in systemMap.values():

				if expressed:
					detected += 1
				else:
					notDetected += 1

			row = [ alleleKey, detected, notDetected, ]

			self.finalResults.append (row)

		self.finalColumns = [ '_Allele_key', 'detectedCount', 'notDetectedCount', ]
		logger.debug ('Found %d alleles' % len(self.finalResults))
		return

###--- globals ---###

# remember the %s at the end of each query, so we can do update-by-key when
# needed
cmds = [
	# items from Cre cache; ordering implies that expressed (1) will
	# override not expressed (0)
	'''select distinct _Allele_key, cresystemlabel, expressed
		from all_cre_cache
		where cresystemlabel is not null
		order by _Allele_key, cresystemlabel, expressed''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Allele_key', 'detectedCount', 'notDetectedCount', ]
logger.debug ('%d requested fields' % len(fieldOrder))

# prefix for the filename of the output file
filenamePrefix = 'allele_recombinase_systems'

# global instance of a AlleleCountsGatherer
gatherer = AlleleRecombinaseSystemGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
