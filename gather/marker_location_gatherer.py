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

NCBI_GENE_MODEL = 59
ENSEMBL_GENE_MODEL = 60
VEGA_GENE_MODEL = 85

###--- Classes ---###

class MarkerLocationGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerLocation table
	# Has: queries to execute against the source database
	# Does: queries the source database for marker locations,
	#	collates results, writes tab-delimited text file

	def getRepSeqProviders (self):
		# get a dictionary mapping from marker keys to the logical
		# database key for the provider of each marker's
		# representative genomic sequence

		cols, rows = self.results[0]
		markerCol = Gatherer.columnNumber (cols, '_Marker_key')
		ldbCol = Gatherer.columnNumber (cols, '_LogicalDB_key')

		d = {}
		for row in rows:
			d[row[markerCol]] = row[ldbCol]
		return d


	def getProviderString (self, markerKey, provider, ldbMap):
		# Identifying the provider for genome coordinates is the
		# result of a set of rules, including:
		# 1. if not a coordinates-based location (with start & stop
		#	coordinates), then no provider
		# 2. if MRK_Location_Cache provider is "NCBI UniSTS" then
		#	provider = "UniSTS"
		# 3. if MRK_Location_Cache provider is "miRBase" then
		#	provider = "miRBase"
		# 4. if MRK_Location_Cache provider is "MGI QTL" then
		#	provider = "MGI"
		# 5. if MRK_Location_Cache provider is "Roopenian STS" then
		#	provider = "MGI"
		# 6. if logical db of rep genome sequence is 59, then
		#	provider = "NCBI"
		# 7. if logical db of rep genome sequence is 60, then
		#	provider ="Ensembl"
		# 8. if logical db of rep genome sequence is 85, then
		#	provider = "VEGA"
		# 9. otherwise provider = "unknown"

		p = provider.lower()

		if p == 'ncbi unists':
			return 'UniSTS'
		elif p == 'mirbase':
			return 'miRBase'
		elif p in ('mgi qtl', 'roopenian sts'):
			return 'MGI'
		
		if ldbMap.has_key(markerKey):
			ldbKey = ldbMap[markerKey]
			if ldbKey == NCBI_GENE_MODEL:
				return 'NCBI'
			elif ldbKey == ENSEMBL_GENE_MODEL:
				return 'Ensembl'
			elif ldbKey == VEGA_GENE_MODEL:
				return 'VEGA'
		return 'unknown'

	def collateResults (self):
		# Purpose: go through the set of query results and assemble
		#	the necessary records in self.finalResults

		genomicSeqLDB = self.getRepSeqProviders()

		self.finalColumns = [ 'markerKey', 'sequenceNum',
			'chromosome', 'cmOffset', 'cytogeneticOffset',
			'startCoordinate', 'endCoordinate', 'buildIdentifier',
			'locationType', 'mapUnits', 'provider', 'strand' ]
		self.finalResults = []

		cols = self.results[-1][0]
		keyCol = Gatherer.columnNumber (cols, '_Marker_key')
		chrCol = Gatherer.columnNumber (cols, 'chromosome')
		cmCol = Gatherer.columnNumber (cols, offset)
		cytoCol = Gatherer.columnNumber (cols, 'cytogeneticOffset')
		startCol = Gatherer.columnNumber (cols, 'startCoordinate')
		endCol = Gatherer.columnNumber (cols, 'endCoordinate')
		buildCol = Gatherer.columnNumber (cols, 'version')
		strandCol = Gatherer.columnNumber (cols, 'strand')
		providerCol = Gatherer.columnNumber (cols, 'provider')

		for row in self.results[-1][1]:
			startCoordinate = row[startCol]
			cm = row[cmCol]
			cyto = row[cytoCol]
			key = row[keyCol]
			chrom = row[chrCol]
			strand = row[strandCol]
			prov = row[providerCol]

			seqNum = 0

			if startCoordinate:
				prov = self.getProviderString (key, prov,
					genomicSeqLDB)

				seqNum = seqNum + 1
				self.finalResults.append ( [ key, seqNum,
					chrom, None, None,
					int(startCoordinate),
					int(row[endCol]), row[buildCol],
					'coordinates', 'bp', prov, strand ] )
			if cm and (int(cm) != -1 or not cyto):
				seqNum = seqNum + 1
				self.finalResults.append ( [ key, seqNum,
					chrom, '%0.2f' % cm, None,
					None, None, None,
					'centimorgans', 'cM', None, None ] )
			if cyto:
				seqNum = seqNum + 1
				self.finalResults.append ( [ key, seqNum,
					chrom, None, cyto,
					None, None, None,
					'cytogenetic', None, None, None ] )
		return

###--- globals ---###

cmds = [
	'''select c._Marker_key, a._LogicalDB_key
	from seq_marker_cache c,
		acc_accession a
	where c._Sequence_key = a._Object_key
		and a._MGIType_key = 19
		and a.preferred = 1
		and c._Qualifier_key = 615419''',

	'''select distinct _Marker_key, chromosome, %s, provider,
		cytogeneticOffset, startCoordinate, endCoordinate, version,
		strand, mapUnits
	from mrk_location_cache''' % offset,
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, 'markerKey', 'sequenceNum', 'chromosome', 'cmOffset',
	'cytogeneticOffset', 'startCoordinate', 'endCoordinate', 
	'buildIdentifier', 'locationType', 'mapUnits', 'provider', 'strand',
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_location'

# global instance of a MarkerLocationGatherer
gatherer = MarkerLocationGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
