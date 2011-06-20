#!/usr/local/bin/python
# 
# gathers data for the 'batch_marker_snps' table in the front-end database

import Gatherer

###--- Classes ---###

class BatchMarkerSnpsGatherer (Gatherer.ChunkGatherer):
	# Is: a data gatherer for the batch_marker_snps table
	# Has: queries to execute against the source database
	# Does: queries the source database for a tiny subset of SNP data
	#	that we need for batch query results that include SNPs,
	#	collates results, writes tab-delimited text file

	def postprocessResults (self):
		self.convertFinalResultsToList()

		lastMarker = None
		markerCol = Gatherer.columnNumber (self.finalColumns,
			'_Marker_key')

		for row in self.finalResults:
			marker = row[markerCol]

			if marker != lastMarker:
				i = 1
				lastMarker = marker
			else:
				i = i + 1

			self.addColumn ('sequence_num', i, row,
				self.finalColumns)
		return

	def getMinKeyQuery (self):
		return 'select min(_Marker_key) from snp_consensussnp_marker'

	def getMaxKeyQuery (self):
		return 'select max(_Marker_key) from snp_consensussnp_marker'


###--- globals ---###

cmds = [
	'''select m._Marker_key, a.accid, a.numericPart
		from snp_consensussnp_marker m,
			snp_accession a
		where m._ConsensusSnp_key = a._Object_key
			and a._MGIType_key = 30
			and m._Marker_key >= %d and m._Marker_key < %d
		order by m._Marker_key, a.numericPart''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', 'accid', 'sequence_num'
	]

# prefix for the filename of the output file
filenamePrefix = 'batch_marker_snps'

# global instance of a BatchMarkerSnpsGatherer
gatherer = BatchMarkerSnpsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
