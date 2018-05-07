#!/usr/local/bin/python
# 
# gathers data for the 'batch_marker_snps' table in the front-end database.
# This table stores the SNP IDs that will be shown in the batch query output
# for each marker (when SNPs are requested).

import Gatherer
import config
import dbAgnostic
import logger

###--- Globals ---###

VALID_MARKERS = None		# set of valid mouse marker keys

###--- Functions ---###

def isValidMarker(markerKey):
	# return True if 'markerKey' is a valid mouse marker, False if not
	global VALID_MARKERS
	
	if not VALID_MARKERS:
		VALID_MARKERS = set()
		cmd = '''select _Marker_key
			from mrk_marker
			where _Organism_key = 1
				and _Marker_Status_key in (1,3)'''
		
		cols, rows = dbAgnostic.execute(cmd)
		for row in rows:
			VALID_MARKERS.add(row[0])
			
		logger.debug('Got %d valid mouse markers' % len(VALID_MARKERS))
		
	return markerKey in VALID_MARKERS

###--- Classes ---###

class BatchMarkerSnpsGatherer (Gatherer.CachingMultiFileGatherer):
	def getValidSNPs (self):
		# get a set of the SNP keys that are considered valid according to their
		# variation class(es)
		
		(cols, rows) = self.results[1]
		snps = set()
		
		for row in rows:
			snps.add(row[0])
		logger.debug('Got %d valid SNPs' % len(snps))
		return snps
	
	def getIDs (self):
		# get a dictionary that maps from SNP key to SNP ID
		
		(cols, rows) = self.results[2]
		keyCol = Gatherer.columnNumber(cols, '_ConsensusSNP_key')
		idCol = Gatherer.columnNumber(cols, 'accID')
		
		snpIDs = {}
		for row in rows:
			snpIDs[row[keyCol]] = row[idCol]
		logger.debug('Got %d SNP IDs' % len(snpIDs))
		return snpIDs
		
	def getMultiCoordSNPs (self):
		# get a set of SNP keys for SNPs with multiple coordinates

		(cols, rows) = self.results[3]
		
		snps = set()
		for row in rows:
			snps.add(row[0])
		logger.debug('Got %d multi-coord SNPs' % len(snps))
		return snps
		
	def collateResults (self):
		# produce the rows of results for the current batch of SNPs
		
#		validSNPs = self.getValidSNPs()
#		snpIDs = self.getIDs()
#		multiCoordSNPs = self.getMultiCoordSNPs()

		(cols, rows) = self.results[-1]
		markerCol = Gatherer.columnNumber(cols, '_Marker_key')
#		snpCol = Gatherer.columnNumber(cols, '_ConsensusSNP_key')
		idCol = Gatherer.columnNumber(cols, 'accID')

		for row in rows:
#			snpKey = row[snpCol]
			self.addRow ('batch_marker_snps', (row[markerCol], row[idCol]))
		return

###--- Setup for Gatherer ---###

tempTable = 'snp_batch'

cmds = [
	# 0 drop any existing temp table
	'drop table if exists %s' % tempTable,
	
	# 1. SNP/marker pairs for the batch that are within the appropriate distance
	'''select distinct s._ConsensusSNP_key, s._Marker_key
		into temp table %s
		from snp_consensussnp_marker s
		where s._ConsensusSNP_key >= %%d
			and s._ConsensusSNP_key < %%d
			and s.distance_from <= 2000
	''' % tempTable, 

	# 2. index the temp table
	'create index %s_index on %s (_ConsensusSNP_key)' % (tempTable, tempTable),
	
	# 3. delete SNPs that have the wrong variation class
	'''delete from %s
		using snp_consensussnp t
		where t._VarClass_key != 1878510
			and %s._ConsensusSNP_key = t._ConsensusSNP_key
	''' % (tempTable, tempTable),
	
	# 4. delete SNPs that have multiple coordinates
	'''delete from %s
		using snp_coord_cache s
		where s.isMultiCoord = 1
			and %s._ConsensusSNP_key = s._ConsensusSNP_key
		''' % (tempTable, tempTable),

	# 5. get SNP/marker pairs
	'''select distinct t._Marker_key, a.accID
		from %s t, snp_accession a
		where t._ConsensusSNP_key = a._Object_key
			and a._MGIType_key = 30		-- consensus SNP
		''' % tempTable,
	]

files = [
	('batch_marker_snps',
		[ '_Marker_key', 'accID' ],
		[ Gatherer.AUTO, '_Marker_key', 'accID' ],
		),
	]

gatherer = BatchMarkerSnpsGatherer (files, cmds)
gatherer.setupChunking (
	'select min(_ConsensusSNP_key) from snp_consensussnp_marker',
	'select max(_ConsensusSNP_key) from snp_consensussnp_marker',
	1750000
	)

###--- main program ---###

if __name__ == '__main__':
	Gatherer.main(gatherer)
