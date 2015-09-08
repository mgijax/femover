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
import dbAgnostic

#
# translate some journal names
#
journalTranslation = {'PLoS One': 'PLoS ONE',
	'Cell Mol Biol (Noisy-le-grand)': 'Cell Mol Biol (Noisy-Le-Grand)'}

###--- Functions ---###

def getGroupingTable():
	# build a temp table which maps from each reference key to its
	# grouping (for filtering -- literature vs. data load, etc.)

	tbl = 'ref_groupings'

	lit = 'Literature'
	nonLit = 'Database, data loads, curation, submissions'

	logger.debug('Building temp table %s' % tbl)

	# begin with assumption that everything is "Literature"

	cmd0 = '''create temporary table %s (
		_Refs_key	int	not null,
		grouping	text	not null,
		primary key (_Refs_key))''' % tbl

	cmd1 = '''insert into %s
		select _Refs_key, '%s' as grouping
		from bib_refs''' % (tbl, lit)

	cmd2 = 'select count(1) from %s' % tbl

	for cmd in [ cmd0, cmd1, cmd2 ]:
		results = dbAgnostic.execute(cmd)

	logger.debug(' - added %d refs to %s' % (results[1][0][0], tbl))

	# flag refs:
	# 1. personal communications
	# 2. null titles
	# 3. database releases, submissions, curation, etc.
	cmd3 = 'update ' + tbl + \
	    " set grouping = '" + nonLit + """'
	    where _Refs_key in (
	    	select _refs_key
		from bib_refs
		where lower(title) like '%personal communic%'
		    or lower(journal) similar to '%database release%|%database proc%|%load%|%personal communic%|%direct data%|%submission%|%curation%|unpublished|%omim%|%www%'
		    or (journal is null and
			(lower(title) similar to '%database%|%load%|%direct data%|%curation%|%annotation%|%nomenclature%|%mouse genome informatics%|%locuslink%'
			or (reftype = 'ART' AND title is null) ) )
	    )"""

	cmd4 = '''select count(1) from %s where grouping = '%s' ''' % (tbl, 
		nonLit)

	for cmd in [ cmd3, cmd4, ]:
		results = dbAgnostic.execute(cmd)

	logger.debug(' - flagged %d refs as "%s"' % (results[1][0][0], nonLit))
	return tbl

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

groupingTable = getGroupingTable()

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
		c.pubmedID,
		g.grouping
	from bib_refs r,
		bib_citation_cache c,
		%s g
	where r._Refs_key = c._Refs_key
		and r._Refs_key = g._Refs_key''' % groupingTable,
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	'referenceKey', 'referenceType', 'primaryAuthor',
	'longAuthors', 'longTitle',
	'journal', 'vol', 'issue', 'pubDate', 'year', 'pages',
	'jnumID', 'numericPart', 'pubmedID', 'miniCitation',
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
