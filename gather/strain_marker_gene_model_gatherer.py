#!/usr/local/bin/python
# 
# gathers data for the 'strain_marker_gene_model' table in the front-end database

import Gatherer
import logger
import FileReader
import dbAgnostic
import Lookup

###--- Globals ---###

###--- Classes ---###

class StrainMarkerGeneModelGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the strain_marker table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for strain markers,
	#	collates results, writes tab-delimited text file

	def getStrainMarkersFromFile(self):
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
			
		strainCol = dbAgnostic.columnNumber(cols, 'strain_key')

		# strip out rows that don't have a strain name
		rowNum = len(rows) - 1
		while rowNum >= 0:
			if not rows[rowNum][strainCol]:
				del rows[rowNum]
			rowNum = rowNum - 1
		
		return cols, rows
	
	def getGeneModelsFromFile(self):
		cols, rows = self.getStrainMarkersFromFile()
		smLookup = {}		# (canonical MGI ID, strain key) : strain marker key
		
		markerCol = dbAgnostic.columnNumber(cols, 'canonical_MGI_ID')
		strainCol = dbAgnostic.columnNumber(cols, 'strain_key')
		smCol = dbAgnostic.columnNumber(cols, 'strain_marker_key')
		
		for row in rows:
			smLookup[(row[markerCol], row[strainCol])] = row[smCol]
		
		logger.debug('Reading more data from file')
		strainMarkerFile = FileReader.FileReader('../data/strain_marker_mini_set.txt')

		cols = [ 'canonical_MGI_ID', 'strain_key', 'strain_geneid' ]
		rows = FileReader.nullify(FileReader.distinct(strainMarkerFile.extract(cols)))

		markerCol = dbAgnostic.columnNumber(cols, 'canonical_MGI_ID')
		strainCol = dbAgnostic.columnNumber(cols, 'strain_key')
		idCol = dbAgnostic.columnNumber(cols, 'strain_geneid')
		
		out = []
		for row in rows:
			strain = row[strainCol]
			if not strain:
				continue

			accid = row[idCol]
			if not accid:
				continue

			logicaldb = None
			if accid.startswith('ENS'):
				logicaldb = 'Ensembl Gene Model'
			elif accid.startswith('MGP'):
				logicaldb = 'MGP'
			else:
				logicaldb = 'NCBI Gene Model'
			
			marker = row[markerCol]

			smKey = smLookup[(marker, strain)]
			out.append([ smKey, accid, logicaldb, len(out) + 1 ])
		return out
	
	def collateResults(self):
		self.finalColumns = [ 'strain_marker_key', 'gene_model_id', 'logical_db', 'sequence_num', ]
		self.finalResults = self.getGeneModelsFromFile()
		logger.debug('Built %d rows with %d cols' % (len(self.finalResults), len(self.finalColumns)))
		return
	
###--- globals ---###

cmds = [
	# 0. token query for now; will need to fill in to pull data from database
	'select 1',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'strain_marker_key', 'gene_model_id', 'logical_db', 'sequence_num', ]

# prefix for the filename of the output file
filenamePrefix = 'strain_marker_gene_model'

# global instance of a StrainMarkerGeneModelGatherer
gatherer = StrainMarkerGeneModelGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
