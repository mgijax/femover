#!/usr/local/bin/python
# 
# gathers data for the 'batch_marker_snps' table in the front-end database

import Gatherer
import config
import MarkerSnpAssociations

###--- Classes ---###

class BatchMarkerSnpsGatherer (Gatherer.ChunkGatherer):
	# Is: a data gatherer for the batch_marker_snps table
	# Has: queries to execute against the source database
	# Does: queries the source database for a tiny subset of SNP data
	#	that we need for batch query results that include SNPs,
	#	collates results, writes tab-delimited text file

	def postprocessResults (self):
		columns = [ '_Marker_key', 'accid', 'sequence_num' ]
		rows = []

		for row in self.finalResults:
			marker = row[0]

			snps = MarkerSnpAssociations.getSnpIDs(marker)

			i = 0

			for snp in snps:
				i = i + 1
				rows.append ( [ marker, snp, i ] )

		self.finalColumns = columns
		self.finalResults = rows
		return

	def getMinKeyQuery (self):
		return 'select min(_Marker_key) from snp_consensussnp_marker'

	def getMaxKeyQuery (self):
		return 'select max(_Marker_key) from snp_consensussnp_marker'


###--- globals ---###

cmds = [ '''select distinct _Marker_key
	from mrk_marker
	where _Organism_key = 1
		and _Marker_Status_key in (1,3)
		and _Marker_key >= %d and _Marker_key < %d''',
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
