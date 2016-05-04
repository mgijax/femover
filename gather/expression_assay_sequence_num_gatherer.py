#!/usr/local/bin/python
# 
# gathers data for the 'expression_assay_sequence_num' table in the front-end
# database

import Gatherer
import logger
import GXDUtils
import ReferenceCitations
import symbolsort

###--- Classes ---###

class AssaySeqNumGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the expression_assay_sequence_num table
	# Has: queries to execute against the source database
	# Does: queries the source database for data needed to order assays,
	#	collates results, writes tab-delimited text file

	def collateResults(self):

		# collect and order markers

		(cols, rows) = self.results[0]

		keyCol = Gatherer.columnNumber (cols, '_Marker_key')
		symbolCol = Gatherer.columnNumber (cols, 'symbol')

		toOrder = []		# [ (symbol, key), ... ]
		
		for row in rows:
			toOrder.append ( (row[symbolCol], row[keyCol]) )

		toOrder.sort(lambda a,b : symbolsort.nomenCompare(a[0], b[0]))

		markerSeqNum = {}
		i = 0

		for (symbol, key) in toOrder:
			i = i + 1
			markerSeqNum[key] = i

		logger.debug ('Ordered %d markers with assays' % i)

		# collate data to be sorted

		bySymbol = []
		byAssayType = []
		byCitation = []

		assayKeys = []

		(cols, rows) = self.results[1]

		assayCol = Gatherer.columnNumber (cols, '_Assay_key')
		markerCol = Gatherer.columnNumber (cols, '_Marker_key')
		refsCol = Gatherer.columnNumber (cols, '_Refs_key')
                typeKeyCol = Gatherer.columnNumber (cols, '_assaytype_key')
		
		for row in rows:
			assayKey = row[assayCol]

			markerPosition = markerSeqNum[row[markerCol]]
			refsPosition = \
				ReferenceCitations.getSequenceNumByMini (
					row[refsCol])
			assayTypePosition = GXDUtils.getAssayTypeSeq(row[typeKeyCol])

			bySymbol.append ( (markerPosition, assayTypePosition,
				refsPosition, assayKey) )
			byAssayType.append ( (assayTypePosition,
				markerPosition, refsPosition, assayKey) )
			byCitation.append ( (refsPosition, markerPosition,
				assayTypePosition, assayKey) )

			assayKeys.append (assayKey)

		logger.debug ('Compiled sorting lists for %d assays' % \
			len(assayKeys) )

		# sort the lists

		bySymbol.sort()
		byAssayType.sort()
		byCitation.sort()
		assayKeys.sort()

		logger.debug ('Sorted lists')

		# assign integers to each assay for each sort type

		assayBySymbol = {}	# key -> integer
		assayByType = {}
		assayByCitation = {}

		for (orderedList, toDict) in [
		    (bySymbol, assayBySymbol),
		    (byAssayType, assayByType),
		    (byCitation, assayByCitation) ]:

			i = 0

			for (x, y, z, assayKey) in orderedList:
				i = i + 1
				toDict[assayKey] = i

		logger.debug ('Converted to dictionaries')

		# collate individually sorted dictionaries into list of final
		# results

		self.finalColumns = [ '_Assay_key', 'by_symbol',
			'by_assay_type', 'by_citation' ]
		self.finalResults = []

		for assayKey in assayKeys:
			self.finalResults.append ( [ assayKey,
				assayBySymbol[assayKey],
				assayByType[assayKey],
				assayByCitation[assayKey] ] )

		logger.debug ('Compiled final results for %d assays' % \
			len(self.finalResults) )
		return

###--- globals ---###

cmds = [
	# 0. symbols for all markers cited in expression assays
	'''select distinct m._Marker_key, m.symbol
		from gxd_assay a,
			mrk_marker m
		where exists (select 1 from gxd_expression e where a._Assay_key = e._Assay_key)
		and a._Marker_key = m._Marker_key''',

	# 1. data needed to order assays
	'''select a._Assay_key,
			a._Marker_key,
			a._Refs_key,
			a._assaytype_key
		from gxd_assay a
		where exists (select 1 from gxd_expression e where a._Assay_key = e._Assay_key)
        ''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Assay_key', 'by_symbol', 'by_assay_type', 'by_citation',
	]

# prefix for the filename of the output file
filenamePrefix = 'expression_assay_sequence_num'

# global instance of a AssaySeqNumGatherer
gatherer = AssaySeqNumGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
