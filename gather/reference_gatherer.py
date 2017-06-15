#!/usr/local/bin/python
# 
# gathers data for the 'reference' table in the front-end database
#
# 06/15/2017	lec
#	- TR12250/LitTriage/reorganize 'grouping'
#

import Gatherer
import ReferenceCitations
import logger
import dbAgnostic

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

		keyCol = Gatherer.columnNumber (self.finalColumns, 'referenceKey')
		authorsCol = Gatherer.columnNumber (self.finalColumns, 'authors')
		titleCol = Gatherer.columnNumber (self.finalColumns, 'title')
		pubmedCol = Gatherer.columnNumber (self.finalColumns, 'pubmedID')
		journalCol = Gatherer.columnNumber (self.finalColumns, 'journal')

		for r in self.finalResults:

			refsKey = r[keyCol]

			miniCitation = ReferenceCitations.getMiniCitation (refsKey)
			shortCitation = ReferenceCitations.getShortCitation (refsKey)
			longCitation = ReferenceCitations.getLongCitation (refsKey)

			self.addColumn ('miniCitation', miniCitation, r, self.finalColumns)
			self.addColumn ('shortCitation', shortCitation, r, self.finalColumns)
			self.addColumn ('longCitation', longCitation, r, self.finalColumns)

			# add the flag for the GXD Literature Index

			if self.gxdRefs.has_key(refsKey):
				inGxdIndex = 1
			else:
				inGxdIndex = 0

			self.addColumn ('indexedForGXD', inGxdIndex, r, self.finalColumns)

		return

###--- globals ---###

#
# _Vocab_key = 131/Refernece Type
# this set will be stored in the "Other" group
# terms not in this set will be stored in the "Literature" group
#
# 'External Resource', 
# 'MGI Curation Record', 
# 'MGI Data Load', 
# 'MGI Direct Data Submission', 
# 'Personal Communication'
# 'Newsletter'
#

cmds = [ '''select distinct _Refs_key from GXD_Index''',

	'''select r._Refs_key as referenceKey,
		c.referenceType,
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
		c.pubmedID,
		c.doiID,
	        'Literature' as grouping
	    from BIB_Refs r, BIB_Citation_Cache c
	    where r._Refs_key = c._Refs_key
	    and c.referenceType not in 
	    	('External Resource', 'MGI Curation Record', 'MGI Data Load', 
		 'MGI Direct Data Submission', 'Personal Communication', 'Newsletter')
	    union
	    select r._Refs_key as referenceKey,
		c.referenceType,
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
		c.pubmedID,
		c.doiID,
		'Other: database loads, direct submissions, etc.' as grouping
	    from BIB_Refs r, BIB_Citation_Cache c
	    where r._Refs_key = c._Refs_key
	    and c.referenceType in 
	    	('External Resource', 'MGI Curation Record', 'MGI Data Load', 
		 'MGI Direct Data Submission', 'Personal Communication')
	'''
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	'referenceKey', 'referenceType', 'primaryAuthor',
	'authors', 'title',
	'journal', 'vol', 'issue', 'pubDate', 'year', 'pages',
	'jnumID', 'numericPart', 'pubmedID', 'doiID', 'miniCitation',
	'shortCitation', 'longCitation', 'indexedForGXD', 'grouping'
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
