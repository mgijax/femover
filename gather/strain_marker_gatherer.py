#!/usr/local/bin/python
# 
# gathers data for the 'strain_marker' table in the front-end database

import Gatherer
import logger
import FileReader
import dbAgnostic
import Lookup

###--- Globals ---###

MARKER_KEY_LOOKUP = Lookup.Lookup('acc_accession', 'accid', '_Object_key', stringSearch = True)
STRAIN_ID_LOOKUP = Lookup.Lookup('acc_accession', '_Object_key', 'accID', initClause = '_MGIType_key = 10')

###--- Classes ---###

class StrainMarkerGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the strain_marker table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for strain markers,
	#	collates results, writes tab-delimited text file

	def getFromFile(self):
		logger.debug('Reading data from file')
		strainMarkerFile = FileReader.FileReader('../data/strain_marker_mini_set.txt')
		cols = [
			'canonical_symbol', 'canonical_MGI_ID', 'strain', 'strain_key', 'strain_chr',
			'strain_start', 'strain_end', 'strain_strand', 'strain_biotype'
			]
		rows = FileReader.nullify(FileReader.distinct(strainMarkerFile.extract(cols)))
		
		# generate a strain key
		cols.append('strain_marker_key')
		key = 1
		for row in rows:
			row.append(key)
			key = key + 1
		logger.debug('Added strain marker keys')
			
		# look up strain keys, strain IDs, and canonical marker keys
		strainCol = dbAgnostic.columnNumber(cols, 'strain')
		markerCol = dbAgnostic.columnNumber(cols, 'canonical_MGI_ID')
		
		cols = cols + [ 'marker_key', 'strain_id' ]
		for row in rows:
			row.append(MARKER_KEY_LOOKUP.get(row[markerCol]))
			row.append(STRAIN_ID_LOOKUP.get(row[-1]))
		logger.debug('Added marker and strain data')

		# strip out rows that don't have a strain name
		rowNum = len(rows) - 1
		while rowNum >= 0:
			if not rows[rowNum][strainCol]:
				del rows[rowNum]
			rowNum = rowNum - 1
		
		return cols, rows
	
	def collateResults(self):
		self.finalColumns = [ 'strain_marker_key', 'canonical_marker_key', 'strain_key',
			'strain_name', 'strain_id', 'feature_type', 'chromosome', 'start_coordinate',
			'end_coordinate', 'strand', 'length', 'sequence_num' ]
		self.finalResults = []
		
		cols, rows = self.getFromFile()
		
		smKeyCol = dbAgnostic.columnNumber(cols, 'strain_marker_key')
		markerKeyCol = dbAgnostic.columnNumber(cols, 'marker_key')
		strainKeyCol = dbAgnostic.columnNumber(cols, 'strain_key')
		strainCol = dbAgnostic.columnNumber(cols, 'strain')
		strainIDCol = dbAgnostic.columnNumber(cols, 'strain_id')
		typeCol = dbAgnostic.columnNumber(cols, 'strain_biotype')
		chromosomeCol = dbAgnostic.columnNumber(cols, 'strain_chr')
		startCoordCol = dbAgnostic.columnNumber(cols, 'strain_start')
		endCoordCol = dbAgnostic.columnNumber(cols, 'strain_end')
		strandCol = dbAgnostic.columnNumber(cols, 'strain_strand')
		
		seqNum = 1
		for row in rows:
			length = None
			if row[startCoordCol] and row[endCoordCol]:
				length = abs(int(row[endCoordCol]) - int(row[startCoordCol])) + 1
				
			r = [ row[smKeyCol], row[markerKeyCol], row[strainKeyCol],
					row[strainCol], row[strainIDCol], row[typeCol], row[chromosomeCol],
					row[startCoordCol], row[endCoordCol], row[strandCol], length, seqNum,
				]
			self.finalResults.append(r)
			seqNum = seqNum + 1
			
		logger.debug('Built %d rows with %d cols' % (len(self.finalResults), len(self.finalColumns)))
		return
	
###--- globals ---###

cmds = [
	# 0. token query for now; will need to fill in to pull data from database
	'select 1',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	'strain_marker_key', 'canonical_marker_key', 'strain_key', 'strain_name', 'strain_id',
	'feature_type', 'chromosome', 'start_coordinate', 'end_coordinate', 'strand', 'length', 'sequence_num',
	]

# prefix for the filename of the output file
filenamePrefix = 'strain_marker'

# global instance of a StrainMarkerGatherer
gatherer = StrainMarkerGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
