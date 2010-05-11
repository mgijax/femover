#!/usr/local/bin/python
# 
# gathers data for the 'referenceAbstract' table in the front-end database

import Gatherer

###--- Classes ---###

class ReferenceAbstractGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the referenceAbstract table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for reference abstracts,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single reference,
		#	rather than for all references

		if self.keyField == 'referenceKey':
			return '_Refs_key = %s' % self.keyValue
		return ''

###--- globals ---###

cmds = [
	'select _Refs_key as referenceKey, abstract from BIB_Refs %s',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [ 'referenceKey', 'abstract', ]

# prefix for the filename of the output file
filenamePrefix = 'referenceAbstract'

# global instance of a ReferenceAbstractGatherer
gatherer = ReferenceAbstractGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
