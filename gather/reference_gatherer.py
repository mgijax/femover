#!/usr/local/bin/python
# 
# gathers data for the 'reference' table in the front-end database

import Gatherer

###--- Functions ---###

def getAuthors (longAuthors):
	# if multiple authors, show only the first for mini and short
	# citations (with trailing comma, space)

	miniCitation = ''
	shortCitation = ''
	longCitation = ''

	if longAuthors:
		semiPos = longAuthors.find(';')
		if semiPos >= 0:
			authors = longAuthors.split(';')[0].strip()
			miniCitation = authors + ', et al., '
		else:
			miniCitation = longAuthors + ', '

		shortCitation = miniCitation 
		longCitation = longAuthors.replace (';', ',') + ', '

	return miniCitation, shortCitation, longCitation

def getArticleCitations (columns, r, longAuthors, longTitle):
	journalCol = Gatherer.columnNumber (columns, 'journal')
	yearCol = Gatherer.columnNumber (columns, 'year')
	dateCol = Gatherer.columnNumber (columns, 'pubDate')
	volumeCol = Gatherer.columnNumber (columns, 'vol')
	issueCol = Gatherer.columnNumber (columns, 'issue')
	pagesCol = Gatherer.columnNumber (columns, 'pages')

	# three different citation types with varying formats

	miniCitation, shortCitation, longCitation = getAuthors (longAuthors)

	# for short and long, include the title (with period, space)

	if longTitle:
		# add a final period to the title if it's missing
		if not longTitle.endswith('.'):
			longTitle = longTitle + '.'

		# add a space
		title = longTitle + ' '

		shortCitation = shortCitation + title
		longCitation = longCitation + title

	# From here on, all three formats are the same.  We will compose this
	# tail portion together in one string and then append it to the
	# individual citations when complete.

	tail = ''

	# journal (abbreviation, period, space)
	# TO DO: switch from journal to its abbreviation

	journalAbbrev = r[journalCol]
	if journalAbbrev:
		tail = tail + journalAbbrev + '. '

	# year (or date, if it exists), then semicolon

	if r[dateCol]:
		date = r[dateCol]
	elif r[yearCol]:
		date = r[yearCol]
	else:
		date = None

	if date:
		tail = tail + date + ';'

	# volume

	if r[volumeCol]:
		tail = tail + r[volumeCol]

	# issue in parentheses

	if r[issueCol]:
		tail = tail + '(' + r[issueCol] + ')'

	# pages preceded by colon

	if r[pagesCol]:
		tail = tail + ':' + r[pagesCol]

	# finish three citations and return them

	miniCitation = miniCitation + tail
	shortCitation = shortCitation + tail
	longCitation = longCitation + tail

	return miniCitation, shortCitation, longCitation

def getBookCitations (columns, r, authors, title, books):

	# three different citation types with varying author formats

	miniCitation, shortCitation, longCitation = getAuthors (authors)

	# get book data

	keyCol = Gatherer.columnNumber (columns, 'referenceKey')
	refsKey = r[keyCol]

	editors = None
	place = None
	bookTitle = None
	publisher = None
	edition = None

	if books.has_key(refsKey):
		editors = books[refsKey]['editors']
		place = books[refsKey]['place']
		bookTitle = books[refsKey]['bookTitle']
		publisher = books[refsKey]['publisher']
		edition = books[refsKey]['edition']

	# chapter title

	if title:
		shortCitation = shortCitation + title
		longCitation = longCitation + title
		if bookTitle:
			shortCitation = shortCitation + ', '
			longCitation = longCitation + ', '
		else:
			shortCitation = shortCitation + '. '
			longCitation = longCitation + '. '

	# book editors 
	
	titleIn = ' in '
	if editors:
		editors = editors.replace (';', ',')
		longCitation = longCitation + ' in ' + editors + ' (eds), '
		titleIn = ''

	# book title 

	if bookTitle:
		miniCitation = miniCitation + ' in ' + bookTitle + '. '
		shortCitation = shortCitation + ' in ' + bookTitle + '. '
		longCitation = longCitation + titleIn + bookTitle + '. '

	# edition

	if edition:
		longCitation = longCitation + edition + '. '

	# date

	yearCol = Gatherer.columnNumber (columns, 'year')
	dateCol = Gatherer.columnNumber (columns, 'pubDate')

	if r[dateCol]:
		date = r[dateCol]
	elif r[yearCol]:
		date = r[yearCol]
	else:
		date = None

	if date:
		miniCitation = miniCitation + date
		shortCitation = shortCitation + date
		longCitation = longCitation + date

	# pages

	pagesCol = Gatherer.columnNumber (columns, 'pages')
	if r[pagesCol]:
		miniCitation = miniCitation + ':' + r[pagesCol] + '.'
		shortCitation = shortCitation + ':' + r[pagesCol] + '.'
		longCitation = longCitation + ':' + r[pagesCol] + '. '

	# place

	if place:
		longCitation = longCitation + place + ': '

	# publisher

	if publisher:
		longCitation = longCitation + publisher + '.'

	return miniCitation, shortCitation, longCitation

###--- Classes ---###

class ReferenceGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the reference table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for references,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		# cache book attributes

		self.books = {}

		columns, results = self.results[0]

		keyCol = Gatherer.columnNumber (columns, '_Refs_key')
		editorsCol = Gatherer.columnNumber (columns, 'editors')
		titleCol = Gatherer.columnNumber (columns, 'book_title')
		placeCol = Gatherer.columnNumber (columns, 'place')
		publisherCol = Gatherer.columnNumber (columns, 'publisher')
		editionCol = Gatherer.columnNumber (columns, 'edition')

		for r in results:
			key = r[keyCol]
			book = {
				'editors' : r[editorsCol],
				'bookTitle' : r[titleCol],
				'place' : r[placeCol],
				'publisher' : r[publisherCol],
				'edition' : r[editionCol],
				}
			self.books[key] = book
		
		# cache which references are cited in the GXD Lit Index

		self.gxdRefs = {}

		cols, rows = self.results[1]
		keyCol = Gatherer.columnNumber (columns, '_Refs_key')
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
		authors2Col = Gatherer.columnNumber (self.finalColumns,
			'authors2')

		titleCol = Gatherer.columnNumber (self.finalColumns, 'title')
		title2Col = Gatherer.columnNumber (self.finalColumns,
			'title2')

		pubmedCol = Gatherer.columnNumber (self.finalColumns,
			'pubmedID')

		for r in self.finalResults:
			# combine two partial authors fields

			longAuthors = None
			if r[authorsCol] != None:
				longAuthors = r[authorsCol]
				if r[authors2Col] != None:
					longAuthors = longAuthors + \
						r[authors2Col]
			self.addColumn ('longAuthors', longAuthors, r,
				self.finalColumns)

			# combine two partial title fields

			longTitle = None
			if r[titleCol] != None:
				longTitle = r[titleCol]
				if r[title2Col] != None:
					longTitle = longTitle + r[title2Col]
				longTitle = longTitle.rstrip()

			self.addColumn ('longTitle', longTitle, r,
				self.finalColumns)

			# determine whether the reference is a book or not

			isBook = False
			typeCol = Gatherer.columnNumber (self.finalColumns,
				'referenceType')
			refType = r[typeCol]
			if refType:
				if refType.upper() == 'BOOK':
					isBook = True

			# get our citations

			if not isBook:
				miniCitation, shortCitation, longCitation = \
					getArticleCitations (
						self.finalColumns, r,
						longAuthors, longTitle)
			else:
				miniCitation, shortCitation, longCitation = \
					getBookCitations (self.finalColumns,
						r, longAuthors, longTitle,
						self.books)

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

			# add the flag for the GXD Literature Index

			if self.gxdRefs.has_key(r[keyCol]):
				inGxdIndex = 1
			else:
				inGxdIndex = 0

			self.addColumn ('indexedForGXD', inGxdIndex, r,
				self.finalColumns)
		return

###--- globals ---###

cmds = [ '''select _Refs_key,
		book_au as editors,
		book_title,
		place,
		publisher,
		series_ed as edition
	from bib_books''',

	'''select distinct _Refs_key
	from gxd_index''',

	'''select r._Refs_key as referenceKey,
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
