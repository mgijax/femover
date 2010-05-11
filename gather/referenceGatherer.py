#!/usr/local/bin/python
# 
# gathers data for the 'reference' table in the front-end database

import Gatherer

###--- Classes ---###

class ReferenceGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the reference table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for references,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single reference,
		#	rather than for all references

		if self.keyField == 'referenceKey':
			return 'r._Refs_key = %s' % \
				self.keyValue
		return ''

	def postprocessResults (self):
		# Purpose: override to combine certain fields

		for r in self.finalResults:
			longAuthors = None
			if r['authors'] != None:
				longAuthors = r['authors']
				if r['authors2'] != None:
					longAuthors = longAuthors + \
						r['authors2']
			r['longAuthors'] = longAuthors

			longTitle = None
			if r['title'] != None:
				longTitle = r['title']
				if r['title2'] != None:
					longTitle = longTitle + r['title2']
			r['longTitle'] = longTitle

###--- globals ---###

cmds = [ '''select r._Refs_key as referenceKey,
		r.refType as referenceType,
		r._primary as primaryAuthor,
		r.authors,
		r.authors2,
		r.title,
		r.title2,
		r.journal,
		r.vol,
		r.issue,
		r.date as pubDate,
		r.year,
		r.pgs as pages,
		c.jnumID,
		c.numericPart,
		c.citation,
		c.short_citation
	from BIB_Refs r,
		BIB_Citation_Cache c
	where r._Refs_key = c._Refs_key %s''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	'referenceKey', 'referenceType', 'primaryAuthor',
	'longAuthors', 'longTitle',
	'journal', 'vol', 'issue', 'pubDate', 'year', 'pages',
	'jnumID', 'numericPart', 'citation', 'short_citation',
	]

# prefix for the filename of the output file
filenamePrefix = 'reference'

# global instance of a ReferenceGatherer
gatherer = ReferenceGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
