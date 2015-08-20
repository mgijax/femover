#!/usr/local/bin/python
# 
# gathers data for the 'allele_imsr_counts' table in the front-end database

import Gatherer
import config
import logger
import httpReader
from IMSRData import IMSRDatabase

###--- Functions ---###

def queryIMSR ():
	imsrDB = IMSRDatabase()
	return imsrDB.queryAllCounts()

###--- Classes ---###

class ImsrGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the allele_imsr_counts table
	# Has: queries to execute against the source database
	# Does: queries the IMSR web site for counts for alleles and their
	#	markers, collates results, writes tab-delimited text file

	def collateResults (self):
		# download data from IMSR
		cellLines, strains, byMarker = queryIMSR()
		logger.debug ('Finished querying IMSR')

		# collect the allele ID for each allele key
		key2id = {}
		columns, rows = self.results[0]
		keyCol = Gatherer.columnNumber (columns, '_Object_key')
		idCol = Gatherer.columnNumber (columns, 'accID')

		for row in rows:
			key2id[row[keyCol]] = row[idCol]

		logger.debug ('Found %d allele IDs' % len(key2id))

		# map from each allele key to the ID of its marker
		alleleToMarker = {}
		columns, rows = self.results[1]
		keyCol = Gatherer.columnNumber (columns, '_Allele_key')
		idCol = Gatherer.columnNumber (columns, 'accID')

		for row in rows:
			id = row[idCol]
			key = row[keyCol]

			# Assumes only one marker per allele
			alleleToMarker[key] = id

		logger.debug ('Found markers for %d alleles' % \
			len(alleleToMarker))

		# mash together the various counts for each allele...
		out = []
		columns = [ '_Allele_key', 'cell_line_count', 'strain_count', 'marker_count' ]

		for allele,alleleID in key2id.items():
			cellLineCount = 0
			strainCount = 0
			byMarkerCount = 0

			if alleleID in cellLines:
				cellLineCount = cellLines[alleleID]

			if alleleID in strains:
				strainCount = strains[alleleID]

			if allele in alleleToMarker:
				markerID = alleleToMarker[allele]
				if markerID in byMarker:
					byMarkerCount = byMarker[markerID]

			out.append([allele,cellLineCount,strainCount,byMarkerCount])

		self.finalColumns = columns
		self.finalResults = out
		logger.debug ('Found counts for %d alleles' % len(out))

###--- globals ---###

cmds = [
	# get the mapping from MGI IDs to allele keys
	'''select accID, _Object_key
		from acc_accession
		where _LogicalDB_key = 1
			and _MGIType_key = 11
			and private = 0''',

	# get the mapping from marker MGI IDs to the markers' allele keys
	'''select a.accID, aa._Allele_key
		from acc_accession a, all_allele aa
		where a._LogicalDB_key = 1
			and a._MGIType_key = 2
			and a.private = 0
			and a.preferred = 1
			and a._Object_key = aa._Marker_key''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	'_Allele_key', 'cell_line_count', 'strain_count', 'marker_count',
	]

# prefix for the filename of the output file
filenamePrefix = 'allele_imsr_counts'

# global instance of a ImsrGatherer
gatherer = ImsrGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
