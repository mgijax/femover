#!/usr/local/bin/python
# 
# gathers data for the 'markerSequenceNum' table in the front-end database

import Gatherer
import logger
import symbolsort
import config

###--- Globals ---###

if config.SOURCE_TYPE == 'sybase':
	offset = 'offset'
else:
	offset = 'cmOffset'

###--- Classes ---###

SYMBOL = 'bySymbol'
TYPE = 'byMarkerType'
ORGANISM = 'byOrganism'
ID = 'byPrimaryID'
LOCATION = 'byLocation'

class MarkerSequenceNumGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerSequenceNum table
	# Has: queries to execute against the source database
	# Does: queries the source database for ordering data for markers,
	#	collates results, writes tab-delimited text file

	def collateResults (self):

		# compute and cache the ordering for marker types

		typeOrder = {}		# typeOrder[type key] = seq num
		i = 0
		keyCol = Gatherer.columnNumber (self.results[1][0],
			'_Marker_Type_key')
		for row in self.results[1][1]:
			i = i + 1
			typeOrder[row[keyCol]] = i

		logger.debug ('Ordered the %d marker types' % len(typeOrder))

		# compute and cache the ordering for organisms

		organismOrder = {}	# organismOrder[org key] = seq num
		i = 0
		orgCol = Gatherer.columnNumber (self.results[2][0],
			'_Organism_key')
		for row in self.results[2][1]:
			i = i + 1
			organismOrder[row[orgCol]] = i

		logger.debug ('Ordered the %d organisms' % len(organismOrder))

		# start collecting the actual data to be sorted...

		# dict[marker key] = [ marker key, sort 1, ... sort n ]
		dict = {}

		# marker symbol (assumes all markers are in this query)

		symbols = []	# list of (lowercase symbol, marker key,
				# marker type key, organism key)

		keyCol = Gatherer.columnNumber (self.results[0][0],
			'_Marker_key')
		typeCol = Gatherer.columnNumber (self.results[0][0],
			'_Marker_Type_key')
		orgCol = Gatherer.columnNumber (self.results[0][0],
			'_Organism_key')
		symCol = Gatherer.columnNumber (self.results[0][0], 'symbol')

		for row in self.results[0][1]:
			# symbol sorting will be case-insensitive
			symbols.append ( (row[symCol].lower(), row[keyCol],
				row[typeCol], row[orgCol]) )

		# sort the tuples by comparing the nomenclature using our
		# special nomen-comparison function (and ignoring the other
		# tuple items)
		symbols.sort (lambda a, b : \
			symbolsort.nomenCompare(a[0], b[0]))

		i = 1
		for (symbol, markerKey, t, o) in symbols:
			dict[markerKey] = [ markerKey, i, typeOrder[t],
				organismOrder[o] ]
			i = i + 1

		self.finalColumns = [ '_Marker_key', SYMBOL, TYPE, ORGANISM ]

		logger.debug ('Sorted %d by symbol' % len(dict))

		# primary ID (assumes sorted results with earlier rows taking
		# precedence)

		allKeys = {}
		for key in dict.keys():
			allKeys[key] = 1

		i = 0
		keyCol = Gatherer.columnNumber (self.results[3][0],
			'_Marker_key')
		for row in self.results[3][1]:
			key = row[keyCol]
			if allKeys.has_key (key):
				i = i + 1
				del allKeys[key]
				dict[key].append (i)

		logger.debug ('Sorted %d by ID' % i)

		# if any markers did not have an ID, then sort to the bottom
		if allKeys:
			i = i + 1
			for key in allKeys.keys():
				dict[key].append (i)

		self.finalColumns.append (ID)

		logger.debug ('Handled %d markers with no ID' % len(allKeys))

		# location (assume we first sort markers with coordinates,
		# then cM offsets, then cytobands.  within coordinates, we
		# sort by chromosome then start coordinate.  likewise for
		# cM offset and cytoband.)

		# to sort markers without coordinates to the end, we need to
		# know 1 more bp than the maximum start coordinate
		maxCoord = self.results[4][1][0][0] + 1

		maxOffset = 99999999	# bigger than any cM offset
		maxCytoband = 'ZZZZ'	# bigger than any cytoband

		locations = []		# list of (chrom seq num, start coord,
					# cM offset, cytoband, marker key)

		columns = self.results[5][0]
		keyCol = Gatherer.columnNumber (columns, '_Marker_key')
		startCol = Gatherer.columnNumber (columns, 'startCoordinate')
		cmCol = Gatherer.columnNumber (columns, offset)
		cytoCol = Gatherer.columnNumber (columns, 'cytogeneticOffset')
		chrCol = Gatherer.columnNumber (columns, 'sequenceNum')

		for row in self.results[5][1]:
			startCoord = row[startCol]
			cmOffset = row[cmCol]
			cytoband = row[cytoCol]

			if startCoord == None:
				startCoord = maxCoord
			if (cmOffset == None) or (cmOffset <= 0):
				cmOffset = maxOffset
			if cytoband == None:
				cytoband == maxCytoband

			locations.append ( (row[chrCol], startCoord,
				cmOffset, cytoband, row[keyCol]) )
		locations.sort()	

		allKeys = {}
		for key in dict.keys():
			allKeys[key] = 1

		i = 0
		for (a,b,c,d, markerKey) in locations:
			if allKeys.has_key (markerKey):
				i = i + 1
				del allKeys[markerKey]
				dict[markerKey].append (i)

		logger.debug ('Sorted %d by location' % i)

		# if any markers did not have a location, then sort to the
		# bottom
		if allKeys:
			i = i + 1
			for key in allKeys.keys():
				dict[key].append (i)

		logger.debug ('Handled %d without locations' % len(allKeys))

		self.finalColumns.append (LOCATION)
		self.finalResults = dict.values() 
		return

###--- globals ---###

cmds = [
	'''select _Marker_key, symbol, _Marker_Type_key,
			_Organism_key
		from mrk_marker''',

	'''select _Marker_Type_key, name
		from mrk_types
		order by name''',

	'''select _Organism_key, commonName
		from mgi_organism
		order by commonName''',

	'''select m._Marker_key, a._LogicalDB_key, a.prefixPart, a.numericPart
		from acc_accession a, acc_logicaldb ldb, mrk_marker m
		where a._MGIType_key = 2
			and a._LogicalDB_key = ldb._LogicalDB_key
			and a.preferred = 1
			and m._Marker_key = a._Object_key
			and ldb._Organism_key = m._Organism_key
		order by m._Marker_key, a._LogicalDB_key, a.prefixPart,
			a.numericPart''',

	'''select max(startCoordinate) as maxStart
		from mrk_location_cache''',

	'''select _Marker_key, chromosome, startCoordinate,
			%s, cytogeneticOffset, sequenceNum
		from mrk_location_cache''' % offset,
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	'_Marker_key', SYMBOL, TYPE, ORGANISM, ID, LOCATION,
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_sequence_num'

# global instance of a MarkerSequenceNumGatherer
gatherer = MarkerSequenceNumGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
