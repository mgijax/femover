#!/usr/local/bin/python
# 
# gathers data for the 'markerLocation' table in the front-end database

import Gatherer
import config

###--- Globals ---###

if config.SOURCE_TYPE == 'sybase':
	offset = 'offset'
else:
	offset = 'cmOffset'

###--- Classes ---###

class MarkerLocationGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerLocation table
	# Has: queries to execute against the source database
	# Does: queries the source database for marker locations,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		# Purpose: go through the set of query results and assemble
		#	the necessary records in self.finalResults

		self.finalColumns = [ 'markerKey', 'sequenceNum',
			'chromosome', 'cmOffset', 'cytogeneticOffset',
			'startCoordinate', 'endCoordinate', 'buildIdentifier',
			'locationType', 'mapUnits', 'provider' ]
		self.finalResults = []

		cols = self.results[0][0]
		keyCol = Gatherer.columnNumber (cols, '_Marker_key')
		chrCol = Gatherer.columnNumber (cols, 'chromosome')
		cmCol = Gatherer.columnNumber (cols, offset)
		cytoCol = Gatherer.columnNumber (cols, 'cytogeneticOffset')
		startCol = Gatherer.columnNumber (cols, 'startCoordinate')
		endCol = Gatherer.columnNumber (cols, 'endCoordinate')
		buildCol = Gatherer.columnNumber (cols, 'version')

		for row in self.results[0][1]:
			startCoordinate = row[startCol]
			cm = row[cmCol]
			cyto = row[cytoCol]
			key = row[keyCol]
			chrom = row[chrCol]

			seqNum = 0

			if startCoordinate:
				seqNum = seqNum + 1
				self.finalResults.append ( [ key, seqNum,
					chrom, None, None,
					int(startCoordinate),
					int(row[endCol]), row[buildCol],
					'coordinates', 'bp', None ] )
			if cm:
				seqNum = seqNum + 1
				self.finalResults.append ( [ key, seqNum,
					chrom, '%0.1f' % cm, None,
					None, None, None,
					'centimorgans', 'cM', None ] )
			if cyto:
				seqNum = seqNum + 1
				self.finalResults.append ( [ key, seqNum,
					chrom, None, cyto,
					None, None, None,
					'cytogenetic', None, None ] )
		return

###--- globals ---###

cmds = [
	'''select distinct _Marker_key, chromosome, %s,
		cytogeneticOffset, startCoordinate, endCoordinate, version
	from mrk_location_cache''' % offset,
	]

# order of fields (from the query results) to be written to the
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
