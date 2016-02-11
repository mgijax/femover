#!/usr/local/bin/python
# 
# gathers data for the 'batch_marker_snps' table in the front-end database.
# This table stores the SNP IDs that will be shown in the batch query output
# for each marker (when SNPs are requested).

import Gatherer
import config

###--- Classes ---###

class BatchMarkerSnpsGatherer (Gatherer.CachingMultiFileGatherer):
	def collateResults (self):
		(cols, rows) = self.results[0]

		markerCol = Gatherer.columnNumber(cols, '_Marker_key')
		idCol = Gatherer.columnNumber(cols, 'accID')

		for row in rows:
			self.addRow ('batch_marker_snps', (row[markerCol], row[idCol]) )
		return

###--- Setup for Gatherer ---###

cmds = [
	# 0. gather the SNPs within 2kb of the markers in this group; only
	# include SNPs with variation class "SNP" for now.
	'''select distinct m._Marker_key, a.accID
		from mrk_marker m, snp_consensussnp_marker s, snp_accession a,
			snp_coord_cache c, snp_consensussnp p
		where m._Marker_key = s._Marker_key
			and m._Marker_key >= %d
			and m._Marker_key < %d
			and s.distance_from <= 2000
			and s._ConsensusSNP_key = a._Object_key
			and a._MGIType_key = 30		-- consensus SNP
			and s._ConsensusSNP_key = c._ConsensusSNP_key
			and c.isMultiCoord = 0
			and s._ConsensusSNP_key = p._ConsensusSNP_key
			and p._VarClass_key = 1878510
	''',
	]

files = [
	('batch_marker_snps',
		[ '_Marker_key', 'accID' ],
		[ Gatherer.AUTO, '_Marker_key', 'accID' ],
		),
	]

gatherer = BatchMarkerSnpsGatherer (files, cmds)
gatherer.setupChunking (
	'select min(_Marker_key) from snp_consensussnp_marker',
	'select max(_Marker_key) from snp_consensussnp_marker',
	10000
	)

###--- main program ---###

if __name__ == '__main__':
	Gatherer.main(gatherer)
