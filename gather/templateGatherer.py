#!/usr/local/bin/python
# 
# gathers data for the 'Template' table in the front-end database
# (search for all instances of Template to see what to change)

import Gatherer

###--- Classes ---###

class TemplateGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the Template table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for Templates,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single Template,
		#	rather than for all Templates

		if self.keyField == 'TemplateKey':
			return 'TemplateTableAbbrev._Template_key = %s' % \
				self.keyValue
		return ''

###--- globals ---###

cmds = [
	# Template - fill in necessary SQL commands here.  Any commands which
	# will require a keyClause to retrieve for an individual object should
	# include ' %s ' in its WHERE section to allow the clause to be
	# automatically inserted.
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	# Template - fill in returned Sybase fields, in desired order, here.
	# If you need the Gatherer to automatically manage a uniqueKey field
	# in the destination table, specify that field as Gatherer.AUTO.
	]

# prefix for the filename of the output file
filenamePrefix = 'Template'

# global instance of a TemplateGatherer
gatherer = TemplateGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
