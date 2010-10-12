#!/usr/local/bin/python
# 
# gathers data for the 'sequence_provider_map' table in the front-end database

import Gatherer

###--- Classes ---###

class SequenceProviderMapGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the sequence_provider_map table
	# Has: queries to execute against the source database
	# Does: caches the mapping from a sequence's logical database to its
	#	provider abbreviation (for seqfetch) in the database

	def postprocessResults (self):
		# provider abbreviations are from the old javawi2 product's
		# FormatHelper.java file (getShortenedProvider method)

		self.convertFinalResultsToList()

		keyCol = Gatherer.columnNumber (self.finalColumns,
			'_LogicalDB_key')

		newResults = []

		for row in self.finalResults:
			ldbKey = row[keyCol]

			if ldbKey == 9:
				abbrev = 'genbank'
			elif ldbKey == 13:
				abbrev = 'swissprot'
			elif ldbKey == 27:
				abbrev = 'refseq'
			elif ldbKey == 36:
				abbrev = 'dotsm'
			elif ldbKey == 41:
				abbrev = 'trembl'
			elif ldbKey == 35:
				abbrev = 'dfcimgi'
			elif ldbKey == 53:
				abbrev = 'niamgi'

			# NCBI, Ensembl, Vega
			elif ldbKey in (59, 60, 85):
				abbrev = 'mousegenome'

			# not sure why these map to foo, but it is the
			# existing behavior
			elif ldbKey in (131, 132, 133, 134):
				abbrev = 'foo'
			
			# if we didn't find an abbreviation, then just skip
			# this row and move on to the next
			else:
				continue

			self.addColumn ('abbrev', abbrev, row,
				self.finalColumns)
			newResults.append (row)

		self.finalResults = newResults
		return

###--- globals ---###

cmds = [
	'select _LogicalDB_key, name from acc_logicaldb',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_LogicalDB_key', 'name', 'abbrev', ]

# prefix for the filename of the output file
filenamePrefix = 'sequence_provider_map'

# global instance of a SequenceProviderMapGatherer
gatherer = SequenceProviderMapGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
