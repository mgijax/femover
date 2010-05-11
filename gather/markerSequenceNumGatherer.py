#!/usr/local/bin/python
# 
# gathers data for the 'markerSequenceNum' table in the front-end database

import Gatherer
import logger
import symbolsort

###--- Classes ---###

class MarkerSequenceNumGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerSequenceNum table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for markers,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single marker,
		#	rather than for all markers

		if self.keyField == 'markerKey':
			return 'm._Marker_key = %s' % \
				self.keyValue
		return ''

	def collateResults (self):
		dict = {}

		# marker symbol
		items = []
		for row in self.results[0]:
			markerKey = row['_Marker_key']
			d = { '_Marker_key' : markerKey,
				'byOrganism' : 0,
				'bySymbol' : 0,
				'byLocation' : 0,
				'byMarkerType' : 0,
				'byPrimaryID' : 0 }
			dict[markerKey] = d

			items.append ( (row['symbol'].lower(), markerKey) )

		items.sort (lambda a, b : symbolsort.nomenCompare(a[0], b[0]))
		i = 1
		for (symbol, markerKey) in items:
			dict[markerKey]['bySymbol'] = i
			i = i + 1
		logger.debug ('Collated symbol data')

		# marker type
		items = []
		for row in self.results[1]:
			items.append ( (row['name'].lower(),
				row['_Marker_Type_key']))
		items.sort()

		byTypeKey = {}
		i = 1
		for (name, key) in items:
			byTypeKey[key] = i
			i = i + 1

		for row in self.results[2]:
			markerKey = row['_Marker_key']
			dict[markerKey]['byMarkerType'] = \
				byTypeKey[row['_Marker_Type_key']]
		logger.debug ('Collated type data')

		# organism
		items = []
		for row in self.results[3]:
			items.append ( (row['commonName'].lower(),
				row['_Organism_key']))
		items.sort()

		byOrganismKey = {}
		i = 1
		for (name, key) in items:
			byOrganismKey[key] = i
			i = i + 1

		for row in self.results[4]:
			markerKey = row['_Marker_key']
			dict[markerKey]['byOrganism'] = \
				byOrganismKey[row['_Organism_key']]
		logger.debug ('Collated organism data')

		# primary ID
		items = []
		for row in self.results[5]:
			items.append ( (row['prefixPart'], row['numericPart'],
				row['_Marker_key']) )
		items.sort()

		i = 1
		for (prefixPart, numericPart, markerKey) in items:
			dict[markerKey]['byPrimaryID'] = i
			i = i + 1
		logger.debug ('Collated primary IDs')

		# location
		maxCoord = self.results[6][0][''] + 1
		maxOffset = 99999999
		maxCytoband = 'ZZZZ'

		items = []
		for row in self.results[7]:
			startCoord = row['startCoordinate']
			offset = row['offset']
			cytoband = row['cytogeneticOffset']

			if startCoord == None:
				startCoord = maxCoord
			if offset == None:
				offset = maxOffset
			if cytoband == None:
				cytoband == maxCytoband

			items.append ( (row['sequenceNum'], startCoord,
				offset, cytoband, row['_Marker_key']) )
		items.sort()	

		i = 1
		for (a,b,c,d, markerKey) in items:
			dict[markerKey]['byLocation'] = i
			i = i + 1
		logger.debug ('Collated locations')

		self.finalResults = dict.values() 
		return

###--- globals ---###

cmds = [
	'select m._Marker_key, m.symbol from MRK_Marker m %s',

	'select t._Marker_Type_key, t.name from MRK_Types t',

	'select m._Marker_key, m._Marker_Type_key from MRK_Marker m %s',

	'select o._Organism_key, o.commonName from MGI_Organism o',

	'select m._Marker_key, m._Organism_key from MRK_Marker m %s',

	'''select m._Marker_key, a.prefixPart, a.numericPart
		from ACC_Accession a, ACC_LogicalDB ldb, MRK_Marker m
		where a._MGIType_key = 2
			and a._LogicalDB_key = ldb._LogicalDB_key
			and a.preferred = 1
			and m._Marker_key = a._Object_key
			and ldb._Organism_key = m._Organism_key %s''',

	'select max(m.startCoordinate) from MRK_Location_Cache m',

	'''select m._Marker_key, m.chromosome, m.startCoordinate,
			m.offset, m.cytogeneticOffset, m.sequenceNum
		from MRK_Location_Cache m %s''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	'_Marker_key', 'bySymbol', 'byMarkerType', 'byOrganism',
	'byPrimaryID', 'byLocation',
	]

# prefix for the filename of the output file
filenamePrefix = 'markerSequenceNum'

# global instance of a MarkerSequenceNumGatherer
gatherer = MarkerSequenceNumGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
