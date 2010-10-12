#!/usr/local/bin/python
# 
# gathers data for the 'allele_imsr_counts' table in the front-end database

import Gatherer
import config
import logger
import httpReader

###--- Functions ---###

def queryIMSR ():
	logger.debug ('IMSR_COUNT_URL : %s' % config.IMSR_COUNT_URL)
	logger.debug ('IMSR_COUNT_TIMEOUT : %d' % config.IMSR_COUNT_TIMEOUT)

	(lines, err) = httpReader.getURL (config.IMSR_COUNT_URL,
		timeout = config.IMSR_COUNT_TIMEOUT)

	if not lines:
		logger.error ('Error reading from IMSR_COUNT_URL: %s' % err)
		raise Gatherer.Error, err

	cellLines = {}
	strains = {}
	byMarker = {}

	for line in lines:
		items = line.split()

		# skip blank lines
		if len(line.strip()) == 0:
			continue

		# report (and skip) lines with too few fields; this would
		# indicate a bug in IMSR
		if len(items) < 3:
			logger.debug (
				'Line from IMSR has too few fields: %s' % \
				line)
			continue

		# look for the three tags we need (other counts for KOMP are
		# included in the same report, so we skip any we don't need)

		id = items[0]
		countType = items[1]
		count = items[2]

		if countType == 'ALL:ES':
			cellLines[id] = count
		elif countType == 'ALL:ST':
			strains[id] = count
		elif countType == 'MRK:UN':
			byMarker[id] = count

	logger.debug ('Cell lines: %d, Strains: %d, byMarker: %d' % (
		len(cellLines), len(strains), len(byMarker) ) )
	return cellLines, strains, byMarker

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
		columns = [ '_Allele_key', 'cell_line_count', 'strain_count',
			'marker_count' ]

		allAlleles = alleleToMarker.keys()
		for allele in allAlleles:
			if not key2id.has_key(allele):
				continue

			id = key2id[allele]

			if cellLines.has_key(id):
				cellLineCount = cellLines[id]
			else:
				cellLineCount = 0

			if strains.has_key(id):
				strainCount = strains[id]
			else:
				strainCount = 0

			markerID = alleleToMarker[allele]
			if byMarker.has_key(markerID):
				byMarkerCount = byMarker[markerID]
			else:
				byMarkerCount = 0

			row = [ allele, cellLineCount, strainCount,
				byMarkerCount ]

			if cellLineCount or strainCount or byMarkerCount:
				out.append (row)

		self.finalColumns = columns
		self.finalResults = out
		logger.debug ('Found counts for %d alleles' % len(out))
		return

###--- globals ---###

cmds = [
	# get the mapping from MGI IDs to allele keys
	'''select accID, _Object_key
		from acc_accession
		where _LogicalDB_key = 1
			and _MGIType_key = 11
			and private = 0''',

	# get the mapping from marker MGI IDs to the markers' allele keys
	'''select a.accID, m._Allele_key
		from acc_accession a, all_marker_assoc m
		where a._LogicalDB_key = 1
			and a._MGIType_key = 2
			and a.private = 0
			and a._Object_key = m._Marker_key''',
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
