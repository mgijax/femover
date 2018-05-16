#!/usr/local/bin/python
# 
# gathers data for annotation tables related to strains.  This currently includes disease
# annotations, but is envisioned as a home for phenotype annotations, too.

import Gatherer
import logger
import StrainUtils
import symbolsort

###--- Globals ---###

GENOTYPE_SORT_MAP = None		# maps from (strain key, genotype key) to sequenceNum

###--- Functions ---###

def compareDiseaseRows(a, b):
	# compare strain_disease rows 'a' and 'b'
	# Assumes: 1. strain key is in position 1
	#	2. disease term is in position 4

	c = cmp(a[1], b[1])
	if c == 0:
		c = symbolsort.nomenCompare(a[4], b[4])
	return c

def compareGenotypeRows(a, b):
	# compare strain_genotype rows 'a' and 'b'
	# Assumes: 1. strain key is in position 1
	#	2. genotype ID is in position 3

	c = cmp(a[1], b[1])
	if c == 0:
		c = symbolsort.nomenCompare(a[3], b[3])
	return c

def compareJoinRows(a, b):
	# compare strain_disease_to_genotype rows 'a' and 'b'
	# Assumes: 1. strain key is in position 0
	#	2. strain/genotype key is in position 1
	#	3. GENOTYPE_SORT_MAP has been populated

	c = cmp(a[0], b[0])
	if c == 0:
		c = cmp(GENOTYPE_SORT_MAP[a[1]], GENOTYPE_SORT_MAP[b[1]])
	return c

###--- Classes ---###

class StrainDiseaseGatherer (Gatherer.CachingMultiFileGatherer):
	# Is: a data gatherer for the strain_disease table
	# Has: queries to execute against the source database
	# Does: queries the source database for diseases associated with strains,
	#	collates results, writes tab-delimited text file

	def lookupColumns (self):
		# look up and cache the column positions for needed fields from the SQL results

		cols, rows = self.results[0]
		self.strainKeyCol = Gatherer.columnNumber(cols, '_Strain_key')
		self.genotypeKeyCol = Gatherer.columnNumber(cols, '_Genotype_key')
		self.genotypeIDCol = Gatherer.columnNumber(cols, 'genotype_id')
		self.qualifierCol = Gatherer.columnNumber(cols, 'qualifier')
		self.diseaseCol = Gatherer.columnNumber(cols, 'disease')
		self.diseaseKeyCol = Gatherer.columnNumber(cols, 'disease_key')
		self.diseaseIDCol = Gatherer.columnNumber(cols, 'disease_id')
		return
		
	def processDiseases(self):
		# pull strain/disease relationships out of the SQL results, generate rows for
		# the strain_disease table, and return a dictionary that maps from 
		#	(strain key, disease key) : strainDiseaseKey
		
		logger.debug('Extracting strain_disease rows')

		sdMap = {}			# maps from (strain key, disease key) : strainDiseaseKey
		sdRows = []			# rows produced for the strain_disease table
		sdKey = 0			# max strainDiseaseKey assigned so far

		cols, rows = self.results[0]
		for row in rows:
			strainKey = row[self.strainKeyCol]
			diseaseKey = row[self.diseaseKeyCol]
			
			# Because of the outer joins, a row may not have a disease.  Skip them in this method.
			if diseaseKey != None:
				pair = (strainKey, diseaseKey)
				if pair not in sdMap:
					sdKey = sdKey + 1
					sdMap[pair] = sdKey
					sdRows.append ( [ sdKey, strainKey, diseaseKey, row[self.diseaseIDCol], row[self.diseaseCol] ] )

		logger.debug(' - got %d rows' % len(sdRows))

		sdRows.sort(compareDiseaseRows)
		logger.debug(' - sorted rows')
		
		i = 0
		for row in sdRows:
			i = i + 1
			row.append(i)
			self.addRow('strain_disease', row)

		logger.debug(' - wrote rows')
		return sdMap
	
	def processGenotypes(self):
		# pull strain/genotype relationships out of the SQL results, generate rows for
		# the strain_genotype table, and return two dictionaries -- one that maps from 
		#	(strain key, genotype key) : strainGenotypeKey
		# and the other that maps from:
		#	strainGenotypeKey : sequence num
		
		logger.debug('Extracting strain_genotype rows')

		sgMap = {}			# maps from (strain key, genotype key) : strainGenotypeKey
		sortMap = {}		# maps from (strain key, genotype key) : sequenceNum
		sgRows = []			# rows produced for the strain_genotype table
		sgKey = 0			# max strainGenotypeKey assigned so far
		hasDisease = {}		# maps from strainGenotypeKey : 1 if that pair has a disease annotation, 0 if not

		cols, rows = self.results[0]
		for row in rows:
			strainKey = row[self.strainKeyCol]
			genotypeKey = row[self.genotypeKeyCol]
			pair = (strainKey, genotypeKey)

			if pair not in sgMap:
				sgKey = sgKey + 1
				sgMap[pair] = sgKey
				sgRows.append ( [ sgKey, strainKey, genotypeKey, row[self.genotypeIDCol] ] )
				hasDisease[sgKey] = 0
				
			if row[self.diseaseKeyCol] != None:
				hasDisease[sgMap[pair]] = 1

		logger.debug(' - got %d rows' % len(sgRows))

		sgRows.sort(compareGenotypeRows)
		logger.debug(' - sorted rows')
		
		# As we walk through the sorted results, we need to:
		# 1. Assign an abbreviation
		# 2. Assign the has_disease flag
		# 3. Assign the sequence_num
		
		seqNum = 0				# last sequence number assigned
		lastStrainKey = None	# last strain key we processed
		
		for row in sgRows:
			seqNum = seqNum + 1
			strainKey = row[1]
			sgKey = row[0]

			if strainKey != lastStrainKey:
				lastStrainKey = strainKey
				model = 1
			else:
				model = model + 1
		
			row.append('model %d' % model)
			row.append(hasDisease[sgKey])
			row.append(seqNum)
			self.addRow('strain_genotype', row)
			sortMap[sgKey] = seqNum

		logger.debug(' - wrote rows')
		return sgMap, sortMap
		
	def processJoinTable(self, diseaseMap, genotypeMap, genotypeSortMap):
		# process the SQL results and produce the join table between diseases and genotypes
		global GENOTYPE_SORT_MAP
		
		logger.debug('Building join table')
		joinRows = []		# list of rows produced for the join table
		
		cols, rows = self.results[0]
		for row in rows:
			strainKey = row[self.strainKeyCol]
			genotypeKey = row[self.genotypeKeyCol]
			diseaseKey = row[self.diseaseKeyCol]

			# no disease?  no row in the join table.
			if diseaseKey != None:
				sgPair = (strainKey, genotypeKey)
				sgKey = genotypeMap[sgPair]

				sdPair = (strainKey, diseaseKey)
				sdKey = diseaseMap[sdPair]
			
				joinRows.append( [ sdKey, sgKey, row[self.qualifierCol] ] )
				
		logger.debug(' - got %d rows' % len(joinRows))
		
		GENOTYPE_SORT_MAP = genotypeSortMap
		joinRows.sort(compareJoinRows)
		logger.debug(' - sorted rows')
		
		seqNum = 0
		for row in joinRows:
			seqNum = seqNum + 1
			row.append(seqNum)
			self.addRow('strain_disease_to_genotype', row)
		
		logger.debug(' - wrote rows')
		return
	
	def collateResults (self):
		# process the SQL results and produce the output data sets
		
		self.lookupColumns()
		diseaseMap = self.processDiseases()
		genotypeMap, genotypeSortMap = self.processGenotypes()
		self.processJoinTable(diseaseMap, genotypeMap, genotypeSortMap)
		return
	
###--- globals ---###

files = [
	('strain_disease',
		[ 'strainDiseaseKey', '_Strain_key', 'disease_key', 'disease_id', 'disease', 'sequenceNum' ],
		[ 'strainDiseaseKey', '_Strain_key', 'disease_key', 'disease_id', 'disease', 'sequenceNum' ]
		),
		
	('strain_genotype',
		[ 'strainGenotypeKey', '_Strain_key', '_Genotype_key', 'genotype_id', 'abbreviation', 'has_disease', 'sequenceNum' ],
		[ 'strainGenotypeKey', '_Strain_key', '_Genotype_key', 'genotype_id', 'abbreviation', 'has_disease', 'sequenceNum' ],
		),
		
	('strain_disease_to_genotype',
		[ 'strainDiseaseKey', 'strainGenotypeKey', 'qualifier', 'sequenceNum' ],
		[ Gatherer.AUTO, 'strainDiseaseKey', 'strainGenotypeKey', 'qualifier', 'sequenceNum' ]
		)
	]
cmds = [
	# 0. strains, genotypes, and diseases.  Note that we should also be able to bring in phenotype
	# data in a subsequent query, tying it to the same set of strain-genotypes.
	'''select distinct gg._Strain_key, gg._Genotype_key, ga.accID as genotype_id,
			q.term as qualifier, d._Term_key as disease_key, da.accID as disease_id, d.term as disease
		from %s s
		inner join gxd_genotype gg on (gg._Strain_key = s._Strain_key)
		inner join acc_accession ga on (gg._Genotype_key = ga._Object_key
			and ga._MGIType_key = 12
			and ga.preferred = 1
			and ga._LogicalDB_key = 1)
		left outer join voc_annot va on (va._AnnotType_key = 1020
			and va._Object_key = gg._Genotype_key)
		left outer join voc_term d on (va._Term_key = d._Term_key)
		left outer join voc_term q on (va._Qualifier_key = q._Term_key)
		left outer join acc_accession da on (va._Term_key = da._Object_key
			and da._MGIType_key = 13
			and da.preferred = 1)
		order by gg._Strain_key, d.term''' % StrainUtils.getStrainTempTable(),
	]

# global instance of a StrainDiseaseGatherer
gatherer = StrainDiseaseGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
