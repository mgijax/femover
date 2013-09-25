#!/usr/local/bin/python
# 
# gathers data for the 'batch_marker_snps' table in the front-end database

import Gatherer
import config
import MarkerSnpAssociations

###--- Classes ---###

class BatchMarkerSnpsGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the batch_marker_snps table
	# Has: queries to execute against the source database
	# Does: queries the source database for a tiny subset of SNP data
	#	that we need for batch query results that include SNPs,
	#	collates results, writes tab-delimited text file

	def postprocessResults (self):
		columns = [ '_Marker_key', 'accid', 'sequence_num' ]
		rows = []

		mrkCol = Gatherer.columnNumber (self.finalColumns,
			'_Marker_key')

		for row in self.finalResults:
			marker = row[mrkCol]

			snps = MarkerSnpAssociations.getSnpIDs(marker)

			i = 0

			for snp in snps:
				i = i + 1
				rows.append ( [ marker, snp, i ] )

			del snps

		self.finalColumns = columns
		self.finalResults = rows
		return

###--- globals ---###

# note there should be no associated SNPs for QTL markers
cmds = [ '''select distinct _Marker_key, chromosome
	from mrk_marker
	where _Organism_key = 1
		and _Marker_Type_key != 6
		and _Marker_Status_key in (1,3)
	order by chromosome''',
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
