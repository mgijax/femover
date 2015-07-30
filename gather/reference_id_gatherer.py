#!/usr/local/bin/python
# 
# gathers data for the 'referenceID' table in the front-end database

import Gatherer

###--- Classes ---###

class ReferenceIDGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the referenceID table
	# Has: queries to execute against source db
	# Does: queries for primary data for reference IDs,
	#	collates results, writes tab-delimited text file

	def postprocessResults (self):
		# Purpose: override to provide key-based lookups

		self.convertFinalResultsToList()

		ldbCol = Gatherer.columnNumber (self.finalColumns,
			'_LogicalDB_key')

		for r in self.finalResults:
			self.addColumn ('logicalDB',
				Gatherer.resolve (r[ldbCol],
					'acc_logicaldb', '_LogicalDB_key',
					'name'),
				r, self.finalColumns)
		return

###--- globals ---###

cmds = [ '''select a._Object_key as referenceKey,
		a._LogicalDB_key,
		a.accID,
		a.preferred,
		a.private
	from acc_accession a
	where a._MGIType_key = 1''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO,
	'referenceKey', 'logicalDB', 'accID', 'preferred', 'private'
	]

# prefix for the filename of the output file
filenamePrefix = 'reference_id'

# global instance of a ReferenceIDGatherer
gatherer = ReferenceIDGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
