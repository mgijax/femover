#!/usr/local/bin/python
# 
# gathers data for the 'allele_recombinase_systems' table in the front-end
# database

import Gatherer
import logger

###--- Globals ---###

# flags for "detected vs not detected" in order by columns in the destination
# table
FLAGS = [
	'adipose tissue',
	'alimentary system',
	'branchial arches',
	'cardiovascular system',
	'cavities and linings',
	'endocrine system',
	'head',
	'hemolymphoid system',
	'integumental system',
	'limbs',
	'liver and biliary system',
	'mesenchyme',
	'muscle',
	'nervous system',
	'renal and urinary system',
	'reproductive system',
	'respiratory system',
	'sensory organs',
	'skeletal system',
	'tail',
	'early embryo',
	'extraembryonic component',
	'embryo other',
	'postnatal other',
	]

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
		expCol = Gatherer.columnNumber (self.results[0][0],
			'expressed')
		sysCol = Gatherer.columnNumber (self.results[0][0],
			'system')

		for row in self.results[0][1]:
			expressed = row[expCol]
			allele = row[keyCol]
			pre = prefix(row[sysCol])

			if not results.has_key(allele):
				results[allele] = {}

			results[allele][pre] = expressed

		logger.debug ('Processed %d rows' % len(self.results[0][1]))

		# at this point, 'results' is complete and we now need to work
		# to compose the final results rows

		self.finalResults = []

		for allele in results.keys():
			flagValues = []
			detected = 0
			notDetected = 0

			for flag in FLAGS:
				pre = prefix(flag)

				if results[allele].has_key(pre):
					flagValue = results[allele][pre]
					if flagValue == 1:
						detected = detected + 1
					else:
						notDetected = notDetected + 1
				else:
					flagValue = None

				flagValues.append (flagValue)

			row = [ allele, ] + flagValues + [ 
				detected, notDetected, ]

			logger.debug ('allele %d, %d flag values, %d cols' % (allele, len(flagValues), len(row)) )
			self.finalResults.append (row)

		self.finalColumns = [ '_Allele_key', ] + FLAGS + [
			'detectedCount', 'notDetectedCount', ]
		logger.debug ('Found %d alleles' % len(self.finalResults))
		return

###--- globals ---###

# remember the %s at the end of each query, so we can do update-by-key when
# needed
cmds = [
	# items from Cre cache; ordering implies that expressed (1) will
	# override not expressed (0)
	'''select distinct _Allele_key, system, expressed
		from all_cre_cache
		order by _Allele_key, system, expressed''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Allele_key', ] + FLAGS + [
	'detectedCount', 'notDetectedCount', ]
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
