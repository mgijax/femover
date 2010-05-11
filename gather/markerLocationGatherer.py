#!/usr/local/bin/python
# 
# gathers data for the 'markerLocation' table in the front-end database

import Gatherer
import copy

###--- Classes ---###

class MarkerLocationGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerLocation table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for marker locations,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single marker,
		#	rather than for all markers

		if self.keyField == 'markerKey':
			return '_Marker_key = %s' % self.keyValue
		return ''

	def collateResults (self):
		# Purpose: go through the set of query results and assemble
		#	the necessary records in self.finalResults

		self.finalResults = []

		for row in self.results[0]:
			startCoordinate = row['startCoordinate']
			cm = row['offset']
			cyto = row['cytogeneticOffset']

			seqNum = 0

			template = {
				'markerKey' : row['_Marker_key'],
				'sequenceNum' : None,
				'chromosome' : None,
				'cmOffset' : None,
				'cytogeneticOffset' : None,
				'startCoordinate' : None,
				'endCoordinate' : None,
				'buildIdentifier' : None,
				'locationType' : None,
				'mapUnits' : None,
				'provider' : None,
				}

			if startCoordinate:
				seqNum = seqNum + 1
				r1 = copy.deepcopy (template)
				r1['chromosome'] = row['chromosome']
				r1['startCoordiante'] = int(startCoordinate)
				r1['endCoordinate'] = \
					int(row['endCoordinate'])
				r1['buildIdentifier'] = row['version']
				r1['sequenceNum'] = seqNum
				r1['locationType'] = 'coordinates'
				r1['mapUnits'] = 'bp'
				self.finalResults.append (r1)

			if cm:
				seqNum = seqNum + 1
				r2 = copy.deepcopy (template)
				r2['chromosome'] = row['chromosome']
				r2['cmOffset'] = '%0.1f' % cm
				r2['locationType'] = 'centimorgans'
				r2['sequenceNum'] = seqNum
				self.finalResults.append (r2)

			if cyto:
				seqNum = seqNum + 1
				r3 = copy.deepcopy (template)
				r3['chromosome'] = row['chromosome']
				r3['cytogeneticOffset'] = cyto
				r3['locationType'] = 'cytogenetic'
				r3['sequenceNum'] = seqNum
				self.finalResults.append (r3)
		return

###--- globals ---###

cmds = [
	'''select distinct _Marker_key, chromosome, offset, 
		cytogeneticOffset, startCoordinate, endCoordinate, version
	from MRK_Location_Cache %s''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, 'markerKey', 'sequenceNum', 'chromosome', 'cmOffset',
	'cytogeneticOffset', 'startCoordinate', 'endCoordinate', 
	'buildIdentifier', 'locationType', 'mapUnits', 'provider',
	]

# prefix for the filename of the output file
filenamePrefix = 'markerLocation'

# global instance of a MarkerLocationGatherer
gatherer = MarkerLocationGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
