#!/usr/local/bin/python
# 
# gathers data for the 'sequenceID' table in the front-end database

import Gatherer

###--- Classes ---###

class SequenceIDGatherer (Gatherer.ChunkGatherer):
	# Is: a data gatherer for the sequenceID table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for sequence IDs,
	#	collates results, writes tab-delimited text file

	def postprocessResults (self):
		# Purpose: override to provide key-based lookups

		self.convertFinalResultsToList()

		ldbCol = Gatherer.columnNumber (self.finalColumns,
			'_LogicalDB_key')

		for r in self.finalResults:
			self.addColumn ('logicalDB', Gatherer.resolve (
				r[ldbCol], 'acc_logicaldb',
				'_LogicalDB_key', 'name'),
				r, self.finalColumns)
		return

	def getMinKeyQuery (self):
		return 'select min(_Sequence_key) from seq_sequence'

	def getMaxKeyQuery (self):
		return 'select max(_Sequence_key) from seq_sequence'

###--- globals ---###

cmds = [
	'''select a._Object_key as sequenceKey, a._LogicalDB_key,
		a.accID, a.preferred, a.private
	from acc_accession a
	where a._MGIType_key = 19
		and a._Object_key >= %d and a._Object_key < %d''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'sequenceKey', 'logicalDB', 'accID',
	'preferred', 'private' ]

# prefix for the filename of the output file
filenamePrefix = 'sequenceID'

# global instance of a SequenceIDGatherer
gatherer = SequenceIDGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
