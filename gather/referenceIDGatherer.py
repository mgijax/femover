#!/usr/local/bin/python
# 
# gathers data for the 'referenceID' table in the front-end database

import Gatherer
import sybaseUtil

###--- Classes ---###

class ReferenceIDGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the referenceID table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for reference IDs,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single reference,
		#	rather than for all references

		if self.keyField == 'referenceKey':
			return 'a._Object_key = %s' % self.keyValue
		return ''

	def postprocessResults (self):
		# Purpose: override to provide key-based lookups

		for r in self.finalResults:
			r['logicalDB'] = sybaseUtil.resolve (
				r['_LogicalDB_key'], 'ACC_LogicalDB', 
				'_LogicalDB_key', 'name')
		return

###--- globals ---###

cmds = [ '''select a._Object_key as referenceKey,
		a._LogicalDB_key,
		a.accID,
		a.preferred,
		a.private
	from ACC_Accession a
	where a._MGIType_key = 1 %s''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO,
	'referenceKey', 'logicalDB', 'accID', 'preferred', 'private'
	]

# prefix for the filename of the output file
filenamePrefix = 'referenceID'

# global instance of a ReferenceIDGatherer
gatherer = ReferenceIDGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
