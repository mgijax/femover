#!/usr/local/bin/python
# 
# gathers data for the 'markerID' table in the front-end database

import Gatherer
import sybaseUtil

###--- Classes ---###

class MarkerIDGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerID table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for marker IDs,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single marker,
		#	rather than for all markers

		if self.keyField == 'markerKey':
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

cmds = [
	'''select a._Object_key as markerKey, a._LogicalDB_key,
		a.accID, a.preferred, a.private
	from ACC_Accession a
	where a._MGIType_key = 2 %s'''
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'markerKey', 'logicalDB', 'accID', 'preferred',
	'private' ]

# prefix for the filename of the output file
filenamePrefix = 'markerID'

# global instance of a markerIDGatherer
gatherer = MarkerIDGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
