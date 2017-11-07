#!/usr/local/bin/python
# 
# gathers data for the 'referenceSequenceNum' table in the front-end database

import Gatherer
import logger

###--- Classes ---###

class ReferenceSequenceNumGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the referenceSequenceNum table
	# Has: queries to execute against the source database
	# Does: queries source db for sorting data for references,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		dict = {}

		refsKeyCol = Gatherer.columnNumber (self.results[0][0],
			'_Refs_key')
		authorsCol = Gatherer.columnNumber (self.results[0][0],
			'authors')
		titleCol = Gatherer.columnNumber (self.results[0][0], 'title')
		yearCol = Gatherer.columnNumber (self.results[0][0], 'year')
		numericPartCol = Gatherer.columnNumber (self.results[0][0],
			'numericPart')

		byDate = []
		byAuthor = []
		byTitle = []
		byID = []
		for row in self.results[0][1]:
			referenceKey = row[refsKeyCol]
			d = { '_Refs_key' : referenceKey,
				'byDate' : 0,
				'byAuthor' : 0,
				'byTitle' : 0,
				'byPrimaryID' : 0 }
			dict[referenceKey] = d

			if row[authorsCol]:
				author = row[authorsCol]
				author = author.lower()
			else:
				author = None

			if row[titleCol]:
				title = row[titleCol]
				title = title.lower()
			else:
				title = None

			numericPart = 0
			if row[numericPartCol] != None:
				numericPart = int(row[numericPartCol])
			
			byDate.append ( (int(row[yearCol]), numericPart, referenceKey) )
			byAuthor.append ( (author, referenceKey) )
			byTitle.append ( (title, referenceKey) )
			byID.append ( (row[numericPartCol], referenceKey) )

		logger.debug ('Pulled out data to sort')

		byDate.sort()
		byAuthor.sort()
		byTitle.sort()
		byID.sort()

		logger.debug ('Sorted data')

		for (lst, field) in [ (byDate, 'byDate'),
			(byAuthor, 'byAuthor'), (byTitle, 'byTitle'),
			(byID, 'byPrimaryID') ]:
				i = 1
				for t in lst:
					referenceKey = t[-1]
					dict[referenceKey][field] = i
					i = i + 1

		self.finalColumns = [ '_Refs_key', 'byDate', 'byAuthor',
			'byTitle', 'byPrimaryID' ]

		self.finalResults = []
		for refDict in dict.values():
			row = [ refDict['_Refs_key'],
				refDict['byDate'],
				refDict['byAuthor'],
				refDict['byTitle'],
				refDict['byPrimaryID'],
				]
			self.finalResults.append (row)
		return

###--- globals ---###

cmds = [
	'''select m._Refs_key, m.authors, m.year, 
		c.numericPart, c.jnumID, m.title
	from bib_refs m, bib_citation_cache c
	where m._Refs_key = c._Refs_key''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	'_Refs_key', 'byDate', 'byAuthor', 'byTitle', 'byPrimaryID',
	]

# prefix for the filename of the output file
filenamePrefix = 'reference_sequence_num'

# global instance of a ReferenceSequenceNumGatherer
gatherer = ReferenceSequenceNumGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
