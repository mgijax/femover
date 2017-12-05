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
	# 0. gather the SNPs within 2kb of the markers in this group, and only include SNPs with
	#	variation class "SNP" for now.
	# Note:  To aid efficiency, I restructured this query to ensure that it was processed in
	#	the order that made sense for the data.  (First find the SNPs based on marker, then
	#	rule out multi-coordinate SNPs and non-SNP variation classes, then find the IDs for
	#	the set that made it through the filtering steps.)  Cuts query runtime by 73%.
	'''with step1 as (
			select s._Marker_key, s._ConsensusSNP_key
			from snp_consensussnp_marker s
			where s._Marker_key >= %d
				and s._Marker_key < %d
				and exists (select 1 from mrk_marker m where m._Marker_key = s._Marker_key)
				and s.distance_from <= 2000
		), step2 as (
			select s._Marker_key, s._ConsensusSNP_key
			from step1 s, snp_coord_cache c, snp_consensussnp t
			where s._ConsensusSNP_key = c._ConsensusSNP_key
				and c.isMultiCoord = 0
				and s._ConsensusSNP_key = t._ConsensusSNP_key
				and t._VarClass_key = 1878510
		)
		select distinct s._Marker_key, a.accID
		from step2 s, snp_accession a
		where s._ConsensusSNP_key = a._Object_key
			and a._MGIType_key = 30		-- consensus SNP
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
