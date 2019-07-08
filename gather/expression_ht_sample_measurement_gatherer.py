#!/usr/local/bin/python
# 
# gathers data for the 'expression_ht_sample_measurement' table in the front-end database
# Note: This data set is too big to fit in memory, so we must deal with it in chunks.

import Gatherer
import logger
import dbAgnostic
import gc
import utils

###--- Globals ---###

SYMBOLS = {}		# marker key : symbol
MARKER_IDS = {}		# marker key : primary ID

###--- Functions ---###

def initialize():
	# initialize this gatherer by populating global caches
	global SYMBOLS, MARKER_IDS
	
	keyCache = '''select distinct _Marker_key
		into temporary table marker_keys
		from gxd_htsample_rnaseq'''
	keyIndex = 'create unique index idx1 on marker_keys(_Marker_key)'

	dbAgnostic.execute(keyCache)
	dbAgnostic.execute(keyIndex)
	logger.debug('Identified unique markers')
	
	symbolQuery = '''select m._Marker_key, m.symbol
		from mrk_marker m
		where m._Organism_key = 1
			and exists (select 1 from marker_keys r where m._Marker_key = r._Marker_key)'''
	utils.fillDictionary('marker symbols', symbolQuery, SYMBOLS, '_Marker_key', 'symbol')
	
	idQuery = '''select a._Object_key, a.accID
		from acc_accession a
		where exists (select 1 from marker_keys r where a._Object_key = r._Marker_key)
			and a._MGIType_key = 2
			and a._LogicalDB_key = 1
			and a.preferred = 1'''
	utils.fillDictionary('marker IDs', idQuery, MARKER_IDS, '_Object_key', 'accID')
	
	dbAgnostic.execute('drop table marker_keys')
	return

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
			markerKey = row[markerKeyCol]
			self.addRow('expression_ht_sample_measurement', [ row[sampleKeyCol], markerKey,
				MARKER_IDS[markerKey], SYMBOLS[markerKey], row[avgTpmCol], row[qnTpmCol]	
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
		[ '_Sample_key', '_Marker_key', 'accID', 'symbol', 'averageTPM', 'qnTPM' ],
		[ Gatherer.AUTO, '_Sample_key', '_Marker_key', 'accID', 'symbol', 'averageTPM', 'qnTPM' ])
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
	initialize()
	Gatherer.main (gatherer)
