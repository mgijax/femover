#!/usr/local/bin/python
# 
# gathers data for the 'strain_snp_cell' and 'strain_snp_row' tables in the front-end database

import Gatherer
import logger
import dbAgnostic
import StrainUtils
import symbolsort
import KeyGenerator

###--- Globals ---###

SS_ROW = 'strain_snp_row'
SS_CELL = 'strain_snp_cell'

STRAIN_SEQ_NUM = {}			# maps from strain key to sequence num
CHROMOSOME_SEQ_NUM = {}		# maps from chromosome to sequence num
STRAIN_NAME = {}			# maps from strain key to strain name
STRAIN_ID = {}				# maps from strain key to strain's primary ID

ROW_KEY_GENERATOR = KeyGenerator.KeyGenerator()	
CELL_KEY_GENERATOR = KeyGenerator.KeyGenerator()	

###--- Functions ---###

def compareRows(a, b):
	# sort data rows a and b, where each is:
	#	[ ref strain key, cmp strain key, chromosome, all count, same count, diff count ]
	# assumes a and b match in column 0, so we sort on columns 1 and 2
	
	c = cmp(STRAIN_SEQ_NUM[a[1]], STRAIN_SEQ_NUM[b[1]])
	if c == 0:
		c = cmp(CHROMOSOME_SEQ_NUM[a[2]], CHROMOSOME_SEQ_NUM[b[2]])
	return c 
	
###--- Classes ---###

class StrainSnpGatherer (Gatherer.CachingMultiFileGatherer):
	# Is: a data gatherer for the strain_snp_row and strain_snp_cell tables
	# Has: queries to execute against the source database
	# Does: queries the source database for SNP counts for strains,
	#	collates results, writes tab-delimited text file

	def collateChromosomes(self):
		# parse query 0 and populate global CHROMOSOME_SEQ_NUM
		
		global CHROMOSOME_SEQ_NUM

		cols, rows = self.results[0]
		chromCol = Gatherer.columnNumber(cols, 'chromosome')
		for row in rows:
			CHROMOSOME_SEQ_NUM[row[chromCol]] = len(CHROMOSOME_SEQ_NUM)

		logger.debug('Collected %d chromosomes' % len(CHROMOSOME_SEQ_NUM))
		return

	def collateStrains(self):
		# parse query 1 and populate globals listed below
		
		global STRAIN_SEQ_NUM, STRAIN_NAME, STRAIN_ID
		
		toSort = []
		cols, rows = self.results[1]
		keyCol = Gatherer.columnNumber(cols, '_Strain_key')
		nameCol = Gatherer.columnNumber(cols, 'strain')
		idCol = Gatherer.columnNumber(cols, 'accID')

		for row in rows:
			toSort.append( (row[nameCol], row[keyCol]) )
			STRAIN_NAME[row[keyCol]] = row[nameCol]
			STRAIN_ID[row[keyCol]] = row[idCol]

		logger.debug('Collected %d strains' % len(STRAIN_SEQ_NUM))
		
		toSort.sort(lambda x, y: symbolsort.nomenCompare(x[0], y[0]))
		for (name, key) in toSort:
			STRAIN_SEQ_NUM[key] = len(STRAIN_SEQ_NUM)

		logger.debug('Sorted %d strains' % len(toSort))
		return
		
	def fillMissingRows(self, refRows):
		# fill in any missing cells (since we only have data for non-zero counts).  Assumes that all
		# rows given are for the same reference strain.
		
		# seen[comparison strain key] = set of chromosomes seen so far
		seen = {}

		for [ refStrainKey, cmpStrainKey, chromosome, allCount, sameCount, diffCount ] in refRows:
			if cmpStrainKey not in seen:
				seen[cmpStrainKey] = set()
			seen[cmpStrainKey].add(chromosome)
			
		ct = 0
		for cmpStrainKey in seen.keys():
			for chromosome in CHROMOSOME_SEQ_NUM.keys():
				if chromosome not in seen[cmpStrainKey]:
					refRows.append( [ refStrainKey, cmpStrainKey, chromosome, 0, 0, 0 ] )
					ct = ct + 1
					
		logger.debug(' - added %d missing cells (zero counts)' % ct) 
		return refRows
	
	def produceOutputRows(self, refRows):
		# take the list of rows for the current reference strain and produce output rows for the
		# data files being written
		
		refRows = self.fillMissingRows(refRows)
		refRows.sort(compareRows)
		logger.debug(' - sorted %d rows' % len(refRows))

		rowCt = 0	# counter of rows added
		cellCt = 0	# counter of cells added
		
		lastCmpStrainKey = None
		for [ refStrainKey, cmpStrainKey, chromosome, allCount, sameCount, diffCount ] in refRows:
			# When we have a new comparison strain, we need to make a new row.
			if (cmpStrainKey != lastCmpStrainKey):
				lastCmpStrainKey = cmpStrainKey
				rowKey = ROW_KEY_GENERATOR.getKey( (refStrainKey, cmpStrainKey) )
				row = [ rowKey, refStrainKey, cmpStrainKey, STRAIN_NAME[cmpStrainKey],
					STRAIN_ID[cmpStrainKey], STRAIN_SEQ_NUM[cmpStrainKey]
					]
				self.addRow(SS_ROW, row)
				rowCt = rowCt + 1
				
			# Now add a new cell to the row
			cellKey = CELL_KEY_GENERATOR.getKey( (refStrainKey, cmpStrainKey, chromosome) )
			cell = [ cellKey, rowKey, chromosome, allCount, sameCount, diffCount, CHROMOSOME_SEQ_NUM[chromosome] ]
			self.addRow(SS_CELL, cell)
			cellCt = cellCt + 1
			
		logger.debug(' - wrote %d cells in %d rows' % (cellCt, rowCt))
		return

	def collateSNPs(self, referenceStrain, comparisonStrains, cacheByStrain):
		# Query the database for counts for the given reference strain with the list of comparison strains.
		# Include any data already cached in 'cacheByStrain' from previous calls.  Need to collect data,
		# generate rows for both database tables, and produce sequence numbers for those rows.  Also need to
		# collect the inverse data (where each comparison strain is treated as a reference strain and the
		# reference strain as the comparison strain) so we can process it later on.  This saves extra work
		# at the database level to compute the came counts with the strains swapped.
		
		logger.debug('Processing strain %d: %d cmp strains' % (referenceStrain, len(comparisonStrains)))

		# list of rows being produced for the reference strain, each row being:
		#	[ ref strain key, cmp strain key, chromosome, all count, same count, diff count ]
		refRows = []
		
		# pull in any cached rows from previous searches, where this reference strain was previously
		# used as a comparison strain
		if referenceStrain in cacheByStrain:
			refRows = cacheByStrain[referenceStrain]
			del cacheByStrain[referenceStrain]
			logger.debug(' - got %d rows from cache' % len(refRows))
		
		# if we have any comparison strains left, then we need to get new records from the database
		if comparisonStrains:
			# need to retrieve allele calls to compare for the same consensus SNP, with the given reference
			# strain and any of the set of comparison strains.  Note that we skip multi-coordinate SNPs, as
			# these are omitted from public display.  Also, when computing counts:
			#	1. in the 'all' count, include any allele calls of:  ACGT?
			#	2. for 'same' or 'different', include only allele calls:  ACGT (exclude ?)
			#	3. all other allele calls are suppressed from public summaries, so exclude them from the counts
			
			cmd = '''select ref._mgdStrain_key as ref_strain_key,
					cmp._mgdStrain_key as cmp_strain_key,
					cc.chromosome,
					count(1) as all_count,
					sum(case when (ref.allele = cmp.allele and ref.allele != '?')
						then 1 else 0 end) as same_count,
					sum(case when (ref.allele != cmp.allele and ref.allele != '?' and cmp.allele != '?')
						then 1 else 0 end) as diff_count
				from snp_consensussnp_strainallele ref, snp_consensussnp_strainallele cmp,
					snp_coord_cache cc
				where ref._ConsensusSNP_key = cmp._ConsensusSNP_key
					and ref._mgdStrain_key = %d
					and cmp._mgdStrain_key in (%s)
					and cmp._ConsensusSNP_key = cc._ConsensusSNP_key
					and ref.allele in ('A', 'C', 'G', 'T', '?')
					and cmp.allele in ('A', 'C', 'G', 'T', '?')
					and cc.isMultiCoord = 0
				group by 1, 2, 3'''
		
			cmpStrains = ','.join(map(str, comparisonStrains))
			cols, rows = dbAgnostic.execute(cmd % (referenceStrain, cmpStrains))
			logger.debug(' - got %d rows from db' % len(rows))
		
			refStrainCol = Gatherer.columnNumber(cols, 'ref_strain_key')
			cmpStrainCol = Gatherer.columnNumber(cols, 'cmp_strain_key')
			chromosomeCol = Gatherer.columnNumber(cols, 'chromosome')
			allCountCol = Gatherer.columnNumber(cols, 'all_count')
			sameCountCol = Gatherer.columnNumber(cols, 'same_count')
			diffCountCol = Gatherer.columnNumber(cols, 'diff_count')

			for row in rows:
				refStrainKey = row[refStrainCol]
				cmpStrainKey = row[cmpStrainCol]
				chromosome = row[chromosomeCol]
				allCount = row[allCountCol]
				sameCount = row[sameCountCol]
				diffCount = row[diffCountCol]

				# collect the row for the reference strain
				refRow = [ refStrainKey, cmpStrainKey, chromosome, allCount, sameCount, diffCount ]
				refRows.append(refRow)

				# cache the same data, but with the strains flipped, for when we process the comparison
				# strain as a reference strain
				cmpRow = [ cmpStrainKey, refStrainKey, chromosome, allCount, sameCount, diffCount ]
				if cmpStrainKey not in cacheByStrain:
					cacheByStrain[cmpStrainKey] = []
				cacheByStrain[cmpStrainKey].append(cmpRow)
			
		self.produceOutputRows(refRows)
		return
	
	def collateResults(self):
		self.collateChromosomes()
		self.collateStrains()

		# cacheByStrain[strain key] = [ list of rows discovered and cached while processing this strain as a comparison strain for another reference strain ]
		# This cache will be built as each reference strain is processed and will be decreased as each strain with cached
		# data itself is processed as a reference strain.
		cacheByStrain = {}

		toDo = STRAIN_SEQ_NUM.keys()
		while len(toDo) > 0:
			referenceStrain = toDo[0]
			toDo = toDo[1:]
			
			self.collateSNPs(referenceStrain, toDo, cacheByStrain)

		logger.debug('Finished processing strains')
		return

###--- globals ---###

cmds = [
	# 0. get unique set of chromosomes having SNPs
	'''with chromosomes as (select distinct chromosome
			from snp_coord_cache)
		select c.chromosome, c.sequenceNum
		from mrk_chromosome c, chromosomes s
		where c._Organism_key = 1
			and s.chromosome = c.chromosome
		order by c.sequenceNum''',
	
	# 1. get unique set of strains with SNPs
	'''with strains as (
			select distinct _mgdStrain_key from snp_consensussnp_strainallele)
		select p._Strain_key, p.strain, a.accID
		from prb_strain p, strains s, %s t, acc_accession a
		where p._Strain_key = s._mgdStrain_key
		and p._Strain_key = t._Strain_key
		and p._Strain_key = a._Object_key
		and a._MGIType_key = 10
		and a._LogicalDB_key = 1
		and a.preferred = 1
		order by p.strain''' % StrainUtils.getStrainTempTable(),
	]

# definitions for output files to be produced
files = [
	(SS_ROW,
		[ 'row_key', 'strain_key', 'comparison_strain_key', 'comparison_strain_name', 'comparison_strain_id', 'sequence_num' ],
		[ 'row_key', 'strain_key', 'comparison_strain_key', 'comparison_strain_name', 'comparison_strain_id', 'sequence_num' ]
		),
	(SS_CELL,
		[ 'cell_key', 'row_key', 'chromosome', 'all_count', 'same_count', 'different_count', 'sequence_num' ],
		[ 'cell_key', 'row_key', 'chromosome', 'all_count', 'same_count', 'different_count', 'sequence_num' ]
		),
	]

# global instance of a StrainSnpGatherer
gatherer = StrainSnpGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
