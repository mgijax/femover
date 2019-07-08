#!/usr/local/bin/python
# 
# gathers data for the 'expression_ht_consolidated_sample_measurement' table in the front-end database

import Gatherer
import utils
import logger
import dbAgnostic
import gc

###--- Globals ---###

SYMBOLS = {}		# marker key : symbol
MARKER_IDS = {}		# marker key : primary ID
LEVELS = {}			# level key : term

###--- Functions ---###

def initialize():
	# initialize this gatherer by populating global caches
	global SYMBOLS, MARKER_IDS, LEVELS
	
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
	
	levelQuery = '''select distinct t._Term_key, t.term
		from voc_term t
		where t._Vocab_key = 144'''
	utils.fillDictionary('levels', levelQuery, LEVELS, '_Term_key', 'term')
	
	dbAgnostic.execute('drop table marker_keys')
	return

###--- Classes ---###

class EHCSMGatherer (Gatherer.CachingMultiFileGatherer):
	# Is: a data gatherer for the expression_ht_consolidated_sample_measurement table
	# Has: queries to execute against the source database
	# Does: queries the source database for measurements for consolidated RNA-Seq samples,
	#	collates results, writes tab-delimited text file

	def collateResults(self):
		cols, rows = self.results[0]
		uniqueKeyCol = dbAgnostic.columnNumber(cols, '_RNASeqCombined_key')
		seqSetKeyCol = dbAgnostic.columnNumber(cols, '_RNASeqSet_key')
		markerKeyCol = dbAgnostic.columnNumber(cols, '_Marker_key')
		levelKeyCol = dbAgnostic.columnNumber(cols, '_Level_key')
		replicatesCol = dbAgnostic.columnNumber(cols, 'numberOfBiologicalReplicates')
		qnTpmCol = dbAgnostic.columnNumber(cols, 'averageQuantileNormalizedTPM')
		
		for row in rows:
			markerKey = row[markerKeyCol]
			self.addRow('expression_ht_consolidated_sample_measurement', [ row[uniqueKeyCol],
				row[seqSetKeyCol], markerKey, MARKER_IDS[markerKey], SYMBOLS[markerKey],
				LEVELS[row[levelKeyCol]], row[replicatesCol], row[qnTpmCol] ] )

		logger.debug('Processed %d rows' % len(rows))
		gc.collect()
		return
	
###--- globals ---###

cmds = [ '''select distinct c._RNASeqCombined_key, m._RNASeqSet_key, r._Marker_key, c._Level_key,
				c.numberOfBiologicalReplicates, c.averageQuantileNormalizedTPM
		from gxd_htsample_rnaseqcombined c, gxd_htsample_rnaseq r, gxd_htsample_rnaseqsetmember m
		where c._RNASeqCombined_key = r._RNASeqCombined_key
			and r._Sample_key = m._Sample_key
			and m._RNASeqSet_key >= %d
			and m._RNASeqSet_key < %d
		'''
	]

# order of fields (from the query results) to be written to the
# output file
files = [
	('expression_ht_consolidated_sample_measurement',
		[ '_RNASeqCombined_key', '_RNASeqSet_key', '_Marker_key', 'marker_id', 'marker_symbol',
			'level', 'numberOfBiologicalReplicates', 'averageQuantileNormalizedTPM' ],
		[ '_RNASeqCombined_key', '_RNASeqSet_key', '_Marker_key', 'marker_id', 'marker_symbol',
			'level', 'numberOfBiologicalReplicates', 'averageQuantileNormalizedTPM' ]
	)
]

# global instance of a TemplateGatherer
gatherer = EHCSMGatherer (files, cmds)
gatherer.setupChunking(
	'select min(_RNASeqSet_key) from gxd_htsample_rnaseqset',
	'select max(_RNASeqSet_key) from gxd_htsample_rnaseqset',
	50
	)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	initialize()
	Gatherer.main (gatherer)
