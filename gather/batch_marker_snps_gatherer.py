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

	def postprocessResults(self):
		columns=['_Marker_key','accid']
		rows=[]
		mrkCol=Gatherer.columnNumber(self.finalColumns,'_Marker_key')

		for row in self.finalResults:
			snps=MarkerSnpAssociations.getSnpIDs(row[mrkCol])
			for snp in snps:
				rows.append([row[mrkCol],snp])
			#del snps

		self.finalColumns = columns
		self.finalResults = rows
		return

###--- globals ---###

# note there should be no associated SNPs for QTL markers
# also omit subtype heritable phenotypic marker (term_key=6238170)
cmds = [ '''select distinct _Marker_key, chromosome
	from mrk_marker m
	where _Organism_key = 1
		and _Marker_Type_key != 6
		and not exists(select 1 from mrk_mcv_cache mcv where mcv._marker_key=m._marker_key and mcv._mcvterm_key=6238170)
		and _Marker_Status_key in (1,3)
	order by chromosome''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', 'accid'
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
