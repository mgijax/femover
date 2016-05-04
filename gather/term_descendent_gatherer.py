#!/usr/local/bin/python
# 
# gathers data for the 'term_descendent' table in the front-end database

import Gatherer
import logger

###--- Classes ---###

class TermDescendentGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the term_descendent table
	# Has: queries to execute against the source database
	# Does: queries the source database for descendents of vocab terms,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		# cache the ID for each vocabulary term, so we can look them
		# up in postprocessResults()

		self.ids = {}		# self.ids[term key] = acc ID

		keyCol = Gatherer.columnNumber (self.results[0][0],
			'_Object_key')
		idCol = Gatherer.columnNumber (self.results[0][0], 'accID')

		for row in self.results[0][1]:
			key = row[keyCol]
			if not self.ids.has_key (key):
				self.ids[key] = row[idCol]

		logger.debug ('cached %d term IDs' % len(self.ids))

		self.finalColumns = self.results[-1][0]
		self.finalResults = self.results[-1][1]
		return

	def postprocessResults (self):
		self.convertFinalResultsToList()

		keyCol = Gatherer.columnNumber (self.finalColumns, 'childKey')

		for row in self.finalResults:
			key = row[keyCol]
			if self.ids.has_key(key):
				id = self.ids[key]
			else:
				id = None

			self.addColumn ('accID', id, row, self.finalColumns)

		return

###--- globals ---###

cmds = [
	'''select _Object_key, accID, _LogicalDB_key
		from acc_accession
		where _MGIType_key = 13
			and private = 0
			and preferred = 1
		order by _LogicalDB_key''',

	'''select dc._AncestorObject_key as parentKey,
			t._Term_key as childKey,
			t.term,
			t.sequenceNum
		from dag_closure dc, voc_term t
		where dc._DescendentObject_key = t._Term_key''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, 'parentKey', 'childKey', 'term', 'accID',
	'sequenceNum',
	]

# prefix for the filename of the output file
filenamePrefix = 'term_descendent'

# global instance of a TermDescendentGatherer
gatherer = TermDescendentGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
