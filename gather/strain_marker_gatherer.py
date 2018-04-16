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
STRAIN_ID_LOOKUP = Lookup.AccessionLookup('Strain')
STRAIN_CACHE = {}		# ID : (name, strain key)

STRAIN_ORDER = [
	'MGI:3028467',	# C57BL/6J
	'MGI:3037980',	# 129S1/SvImJ
	'MGI:2159747',	# A/J
	'MGI:2159745',	# AKR/J
	'MGI:2159737',	# BALB/cJ
	'MGI:2159741', 	# C3H/HeJ
	'MGI:3056279',	# C57BL/6NJ
	'MGI:2159793',	# CAST/EiJ
	'MGI:2159756',	# CBA/J
	'MGI:2684695',	# DBA/2J
	'MGI:2163709',	# FVB/NJ
	'MGI:2159761',	# LP/J
	'MGI:2160559',	# M. caroli
	'MGI:5651824',	# M. pahari
	'MGI:2162056',	# NOD/ShiLtJ
	'MGI:2173835',	# NZO/HlLtJ
	'MGI:2160654',	# PWK/PhJ
	'MGI:2160671',	# SPRET/EiJ
	'MGI:2160667',	# WSB/EiJ
	]

###--- Functions ---###

def getStrainSequenceNum(strainID):
	if strainID in STRAIN_ORDER:
		return STRAIN_ORDER.index(strainID)
	return len(STRAIN_ORDER) + 1

def getStrain(strainID):
	# returns (name, key) for given strain ID
	global STRAIN_CACHE
	
	if strainID not in STRAIN_CACHE:
		cmd = '''select ps.strain, ps._Strain_key
			from acc_accession a, prb_strain ps
			where a._MGIType_key = 10
			and a.accID = '%s'
			and a._Object_key = ps._Strain_key''' % strainID
		
		(cols, rows) = dbAgnostic.execute(cmd)
		STRAIN_CACHE[strainID] = rows[0]

	return STRAIN_CACHE[strainID]

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
		strainKeyCol = dbAgnostic.columnNumber(cols, 'strain_key')
		markerCol = dbAgnostic.columnNumber(cols, 'canonical_MGI_ID')
		
		cols = cols + [ 'marker_key', 'strain_id' ]
		for row in rows:
			row.append(MARKER_KEY_LOOKUP.get(row[markerCol]))
			row.append(STRAIN_ID_LOOKUP.get(row[strainKeyCol]))
		logger.debug('Added marker and strain data')

		# strip out rows that don't have a strain name
		rowNum = len(rows) - 1
		while rowNum >= 0:
			if not rows[rowNum][strainCol]:
				del rows[rowNum]
			rowNum = rowNum - 1
		
		return cols, rows
	
	def findMissingStrainsPerMarker(self):
		# returns { marker key : list of strain IDs with no data }
		
		markerCol = dbAgnostic.columnNumber(self.finalColumns, 'canonical_marker_key')
		strainCol = dbAgnostic.columnNumber(self.finalColumns, 'strain_id')
		
		existing = {}
		for row in self.finalResults:
			markerKey = row[markerCol]
			strainID = row[strainCol]

			if markerKey not in existing:
				existing[markerKey] = set()
			existing[markerKey].add(strainID)
			
		markers = existing.keys()
		markers.sort()
		
		ct = 0
		missing = {}
		for markerKey in markers:
			for strainID in STRAIN_ORDER:
				if strainID not in existing[markerKey]:
					if markerKey not in missing:
						missing[markerKey] = []
					missing[markerKey].append(strainID)
					ct = ct + 1

		logger.debug('Found %d missing rows for %d markers' % (ct, len(missing)))
		return missing

	def generateMissingStrainMarkers(self):
		# generate strain/marker rows for strains where a marker has no data
		
		missing = self.findMissingStrainsPerMarker()
		markers = missing.keys()
		markers.sort()
		smKey = self.finalResults[-1][0]			# last strain/marker key assigned
		ct = 0
		
		for markerKey in markers:
			for strainID in missing[markerKey]:
				strain, strainKey = getStrain(strainID)
				seqNum = getStrainSequenceNum(strainID)

				smKey = smKey + 1
				r = [ smKey, markerKey, strainKey, strain, strainID,
						None, None, None, None, None, None, seqNum
					]
				self.finalResults.append (r)
				ct = ct + 1

		logger.debug('Generated %d filler rows' % ct)
		return
	
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
		
		for row in rows:
			length = None
			if row[startCoordCol] and row[endCoordCol]:
				length = abs(int(row[endCoordCol]) - int(row[startCoordCol])) + 1
				
			seqNum = getStrainSequenceNum(row[strainIDCol])
			r = [ row[smKeyCol], row[markerKeyCol], row[strainKeyCol],
					row[strainCol], row[strainIDCol], row[typeCol], row[chromosomeCol],
					row[startCoordCol], row[endCoordCol], row[strandCol], length, seqNum,
				]
			self.finalResults.append(r)
			
		logger.debug('Built %d rows with %d cols' % (len(self.finalResults), len(self.finalColumns)))
		self.generateMissingStrainMarkers()
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
