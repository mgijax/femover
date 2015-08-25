#!/usr/local/bin/python
# 
# gathers data for the 'reference' table in the front-end database
#
# 05/02/2013	lec
#	- TR11248 (cre/gxd/snp): add journalTranslation dictionary
#

import Gatherer
import ReferenceCitations
import logger

#
# translate some journal names
#
journalTranslation = {'PLoS One': 'PLoS ONE',
	'Cell Mol Biol (Noisy-le-grand)': 'Cell Mol Biol (Noisy-Le-Grand)'}


###--- Classes ---###

class ReferenceGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the reference table
	# Has: queries to execute against source db
	# Does: queries for primary data for references,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		
		# cache which references are cited in the GXD Lit Index

		self.gxdRefs = {}

		cols, rows = self.results[0]
		keyCol = Gatherer.columnNumber (cols, '_Refs_key')
		for row in rows:
			self.gxdRefs[row[keyCol]] = 1 

		# use last query as base for our final results

		self.finalColumns = self.results[-1][0]
		self.finalResults = self.results[-1][1]
		return

	def postprocessResults (self):
		# Purpose: override to combine certain fields

		self.convertFinalResultsToList()

		keyCol = Gatherer.columnNumber (self.finalColumns,
			'referenceKey')
		authorsCol = Gatherer.columnNumber (self.finalColumns,
			'authors')

		titleCol = Gatherer.columnNumber (self.finalColumns, 'title')

		pubmedCol = Gatherer.columnNumber (self.finalColumns,
			'pubmedID')

		journalCol = Gatherer.columnNumber (self.finalColumns, 'journal')

		for r in self.finalResults:
			# combine two partial authors fields

			longAuthors = None
			if r[authorsCol] != None:
				longAuthors = r[authorsCol]
			self.addColumn ('longAuthors', longAuthors, r,
				self.finalColumns)

			# combine two partial title fields

			longTitle = None
			if r[titleCol] != None:
				longTitle = r[titleCol]
				longTitle = longTitle.rstrip()

			self.addColumn ('longTitle', longTitle, r,
				self.finalColumns)

			# get our citations

			refsKey = r[keyCol]

			miniCitation = ReferenceCitations.getMiniCitation (
				refsKey)
			shortCitation = ReferenceCitations.getShortCitation (
				refsKey)
			longCitation = ReferenceCitations.getLongCitation (
				refsKey)

			self.addColumn ('miniCitation', miniCitation, r,
				self.finalColumns)
			self.addColumn ('shortCitation', shortCitation, r,
				self.finalColumns)
			self.addColumn ('longCitation', longCitation, r,
				self.finalColumns)

			# clean up PubMed IDs which are the string "null"
			# rather than a null value

			if (r[pubmedCol] != None) and \
				(r[pubmedCol].lower() == 'null'):
					r[pubmedCol] = None

			# translate journal names (if necessary)
			journal = r[journalCol]
			if (journalTranslation.has_key(journal)):
				logger.debug(r[journalCol])
				r[journalCol] = journalTranslation[journal]
				logger.debug(r[journalCol])

			# add the flag for the GXD Literature Index

			if self.gxdRefs.has_key(refsKey):
				inGxdIndex = 1
			else:
				inGxdIndex = 0

			self.addColumn ('indexedForGXD', inGxdIndex, r,
				self.finalColumns)
		return

###--- globals ---###

cmds = [ '''select distinct _Refs_key
	from gxd_index''',

	'''select r._Refs_key as referenceKey,
		r.refType as referenceType,
		r._primary as primaryAuthor,
		r.authors,
		r.title,
		r.journal,
		lower(r.journal) as lowerJournal,
		r.vol,
		r.issue,
		r.date as pubDate,
		r.year,
		r.pgs as pages,
		c.jnumID,
		c.numericPart,
		c.pubmedID
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
	'jnumID', 'numericPart', 'pubmedID', 'miniCitation',
	'shortCitation', 'longCitation', 'indexedForGXD',
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
