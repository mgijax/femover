#!/usr/local/bin/python
# 
# gathers data for the 'sequenceID' table in the front-end database

import Gatherer
import sybaseUtil

###--- Classes ---###

class SequenceIDGatherer (Gatherer.ChunkGatherer):
	# Is: a data gatherer for the sequenceID table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for sequence IDs,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single sequence,
		#	rather than for all sequences

		if self.keyField == 'sequenceKey':
			return 'a._Object_key = %s' % self.keyValue
		return ''

	def postprocessResults (self):
		# Purpose: override to provide key-based lookups

		for r in self.finalResults:
			r['logicalDB'] = sybaseUtil.resolve (
				r['_LogicalDB_key'], 'ACC_LogicalDB',
				'_LogicalDB_key', 'name')
		return

	def getMinKeyQuery (self):
		return 'select min(_Sequence_key) from SEQ_Sequence'

	def getMaxKeyQuery (self):
		return 'select max(_Sequence_key) from SEQ_Sequence'

	def getKeyRangeClause (self):
		return 'a._Object_key >= %d and a._Object_key < %d'

###--- globals ---###

cmds = [
	'''select a._Object_key as sequenceKey, a._LogicalDB_key,
		a.accID, a.preferred, a.private
	from ACC_Accession a
	where a._MGIType_key = 19 %s'''
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'sequenceKey', 'logicalDB', 'accID', 'preferred',
	'private' ]

# prefix for the filename of the output file
filenamePrefix = 'sequenceID'

# global instance of a SequenceIDGatherer
gatherer = SequenceIDGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
