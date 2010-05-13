#!/usr/local/bin/python
# 
# gathers data for the 'referenceSequenceNum' table in the front-end database

import Gatherer
import logger

###--- Classes ---###

class ReferenceSequenceNumGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the referenceSequenceNum table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for references,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single reference,
		#	rather than for all references

		if self.keyField == 'referenceKey':
			return 'm._Reference_key = %s' % self.keyValue
		return ''

	def collateResults (self):
		dict = {}

		byDate = []
		byAuthor = []
		byTitle = []
		byID = []
		for row in self.results[0]:
			referenceKey = row['_Refs_key']
			d = { '_Refs_key' : referenceKey,
				'byDate' : 0,
				'byAuthor' : 0,
				'byTitle' : 0,
				'byPrimaryID' : 0 }
			dict[referenceKey] = d

			if row['authors']:
				author = row['authors']
				if row['authors2']:
					author = author + row['authors2']
				author = author.lower()
			else:
				author = None

			if row['title']:
				title = row['title']
				if row['title2']:
					title = title + row['title2']
				title = title.lower()
			else:
				title = None

			byDate.append ( (int(row['year']),
				int(row['numericPart']), referenceKey) )
			byAuthor.append ( (author, referenceKey) )
			byTitle.append ( (title, referenceKey) )
			byID.append ( (row['numericPart'],
				referenceKey) )

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

		self.finalResults = dict.values() 
		return

###--- globals ---###

cmds = [
	'''select m._Refs_key, m.authors, m.authors2, m.year, 
		c.numericPart, c.jnumID, m.title, m.title2
	from BIB_Refs m, BIB_Citation_Cache c
	where m._Refs_key = c._Refs_key %s''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	'_Refs_key', 'byDate', 'byAuthor', 'byTitle', 'byPrimaryID',
	]

# prefix for the filename of the output file
filenamePrefix = 'referenceSequenceNum'

# global instance of a ReferenceSequenceNumGatherer
gatherer = ReferenceSequenceNumGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
