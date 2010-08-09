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

	def postprocessResults (self):
		# Purpose: override to combine certain fields

		self.convertFinalResultsToList()

		authorsCol = Gatherer.columnNumber (self.finalColumns,
			'authors')
		authors2Col = Gatherer.columnNumber (self.finalColumns,
			'authors2')
		titleCol = Gatherer.columnNumber (self.finalColumns, 'title')
		title2Col = Gatherer.columnNumber (self.finalColumns,
			'title2')

		for r in self.finalResults:
			longAuthors = None
			if r[authorsCol] != None:
				longAuthors = r[authorsCol]
				if r[authors2Col] != None:
					longAuthors = longAuthors + \
						r[authors2Col]
			self.addColumn ('longAuthors', longAuthors, r,
				self.finalColumns)

			longTitle = None
			if r[titleCol] != None:
				longTitle = r[titleCol]
				if r[title2Col] != None:
					longTitle = longTitle + r[title2Col]
			self.addColumn ('longTitle', longTitle, r,
				self.finalColumns)
		return

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
		c.pubmedID,
		c.citation,
		c.short_citation
	from bib_refs r,
		bib_citation_cache c
	where r._Refs_key = c._Refs_key''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	'referenceKey', 'referenceType', 'primaryAuthor',
	'longAuthors', 'longTitle',
	'journal', 'vol', 'issue', 'pubDate', 'year', 'pages',
	'jnumID', 'numericPart', 'pubmedID', 'citation', 'short_citation',
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
