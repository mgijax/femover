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
		
		validSNPs = self.getValidSNPs()
		snpIDs = self.getIDs()
		multiCoordSNPs = self.getMultiCoordSNPs()

		(cols, rows) = self.results[0]
		markerCol = Gatherer.columnNumber(cols, '_Marker_key')
		snpCol = Gatherer.columnNumber(cols, '_ConsensusSNP_key')

		for row in rows:
			snpKey = row[snpCol]
			if (snpKey not in validSNPs) or (snpKey not in snpIDs) or (snpKey in multiCoordSNPs):
				continue
			self.addRow ('batch_marker_snps', (row[markerCol], snpIDs[snpKey]))
		return

###--- Setup for Gatherer ---###

cmds = [
	# 0. SNP/marker pairs for the batch that are within the appropriate distance
	'''select distinct s._ConsensusSNP_key, s._Marker_key
		from snp_consensussnp_marker s
		where s._ConsensusSNP_key >= %d
			and s._ConsensusSNP_key < %d
			and s.distance_from <= 2000
	''',
	
	# 1. SNPs for the batch that have the appropriate variation class(es)
	'''select t._ConsensusSNP_key
		from snp_consensussnp t
		where t._ConsensusSNP_key >= %d
			and t._ConsensusSNP_key < %d
			and t._VarClass_key = 1878510
	''',
	
	# 2. accession IDs for SNPs in the batch
	'''select distinct a._Object_key as _ConsensusSNP_key, a.accID
		from snp_accession a
		where a._Object_key >= %d
			and a._Object_key < %d
			and a._MGIType_key = 30		-- consensus SNP
	''',
	
	# 3. SNPs that have multiple coordinates (to skip)
	'''select _ConsensusSNP_key
		from snp_coord_cache
		where _ConsensusSNP_key >= %d
			and _ConsensusSNP_key < %d
			and isMultiCoord = 1''',
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
