#!/usr/local/bin/python
# 
# gathers data for the 'expression_ht_sample_measurement' table in the front-end database
# Note: This data set is too big to fit in memory, so we must deal with it in chunks.

import Gatherer
import logger
import dbAgnostic
import gc

###--- Classes ---###

class EHSMGatherer (Gatherer.CachingMultiFileGatherer):
	# Is: a data gatherer for the expression_ht_sample_measurement table
	# Has: queries to execute against the source database
	# Does: queries the source database for measurements for RNA-Seq samples,
	#	collates results, writes tab-delimited text file

	def collateResults(self):
		# slice and dice the query results to produce our set of final results
		# for this chunk of _RNASeq_keys
		
		cols, rows = self.results[0]
		sampleKeyCol = dbAgnostic.columnNumber(cols, '_Sample_key')
		markerKeyCol = dbAgnostic.columnNumber(cols, '_Marker_key')
		avgTpmCol = dbAgnostic.columnNumber(cols, 'averageTPM')
		qnTpmCol = dbAgnostic.columnNumber(cols, 'qnTPM')
		
		for row in rows:
			self.addRow('expression_ht_sample_measurement', [ row[sampleKeyCol], row[markerKeyCol],
				row[avgTpmCol], row[qnTpmCol]	
				] )

		logger.debug('Processed %d rows' % len(rows))
		gc.collect()
		return
		
###--- globals ---###

cmds = [
	# 0. walk through the RNA-Seq records, which are clustered by _RNASeq_key
	'''select r._Sample_key, r._Marker_key, r.averageTPM, r.quantileNormalizedTPM as qnTPM
		from gxd_htsample_rnaseq r
		where r._RNASeq_key >= %d
			and r._RNASeq_key < %d
	'''
	]

# order of fields (from the query results) to be written to the
# output file
files = [
	('expression_ht_sample_measurement',
		[ '_Sample_key', '_Marker_key', 'averageTPM', 'qnTPM' ],
		[ Gatherer.AUTO, '_Sample_key', '_Marker_key', 'averageTPM', 'qnTPM' ])
	]

# prefix for the filename of the output file
filenamePrefix = 'expression_ht_sample_measurement'

# global instance of a EHSMGatherer
gatherer = EHSMGatherer (files, cmds)
gatherer.setupChunking (
	'select min(_RNASeq_key) from GXD_HTSample_RNASeq',
	'select max(_RNASeq_key) from GXD_HTSample_RNASeq',
	100000
	)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
