#!/usr/local/bin/python
# 
# gathers data for the 'markerToReference' table in the front-end database

import Gatherer

###--- Classes ---###

class MarkerToReferenceGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerToReference table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for marker/reference
	#	associations, collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single marker or
		#	reference, rather than for all records

		if self.keyField == 'markerKey':
			return '_Marker_key = %s' % self.keyValue
		elif self.keyField == 'refsKey':
			return '_Refs_key = %s' % self.keyValue
		return ''

###--- globals ---###

cmds = [ '''select _Marker_key, _Refs_key from MRK_Reference %s''', ]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Marker_key', '_Refs_key', ]

# prefix for the filename of the output file
filenamePrefix = 'markerToReference'

# global instance of a MarkerToReferenceGatherer
gatherer = MarkerToReferenceGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
