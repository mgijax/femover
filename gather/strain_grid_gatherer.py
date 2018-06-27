#!/usr/local/bin/python
# 
# gathers data for the 'strain_grid_*' tables in the front-end database

import Gatherer
import logger
import DiseasePortalUtils
import KeyGenerator
import StrainUtils
import Lookup
import VocabUtils
import symbolsort

###--- Globals ---###

# names of the six tables we're generating
SG_CELL = 'strain_grid_cell'
SG_HEADING = 'strain_grid_heading'
SG_HEADING_TO_TERM = 'strain_grid_heading_to_term'
SG_POPUP_ROW = 'strain_grid_popup_row'
SG_POPUP_COLUMN = 'strain_grid_popup_column'
SG_POPUP_CELL = 'strain_grid_popup_cell'

# generates heading_key values based on heading term
HEADING_KEY_GENERATOR = KeyGenerator.KeyGenerator()	

# generates grid_cell_key values based on strain key and heading key
GRIDCELL_KEY_GENERATOR = KeyGenerator.KeyGenerator()

# generates row_key values based on grid cell key and genotype key
ROW_KEY_GENERATOR = KeyGenerator.KeyGenerator(bidirectional = True)

# generates column_key values based on grid cell key and annotated term
COLUMN_KEY_GENERATOR = KeyGenerator.KeyGenerator(bidirectional = True)

###--- Functions ---###

def compareGenotypeTuple(a, b):
	# compare two genotype tuples (allele pairs, genotype key) for sorting

	c = symbolsort.nomenCompare(a[0], b[0])
	if c == 0:
		c = cmp(a[1], b[1])
	return c

###--- Classes ---###

class MPHeaderMap:
	# object for mapping various bits of MP Header terms to one another, using in-memory
	# caches for efficiency
	
	def __init__ (self):
		self.keys = {}		# header term -> term key
		self.ids = {}		# header term -> ID
		self.terms = {}		# header term key -> header term
		self.abbrev = {}	# header term key -> header term abbreviation
		self.idLookup = Lookup.AccessionLookup('Vocabulary Term', logicalDB = 'Mammalian Phenotype')
		return
	
	def getKey(self, headerTerm):
		if headerTerm not in self.keys:
			self.keys[headerTerm] = DiseasePortalUtils.getMPHeaderKey(headerTerm)
		return self.keys[headerTerm]
	
	def getID(self, headerTerm):
		if headerTerm not in self.ids:
			self.ids[headerTerm] = self.idLookup.get(self.getKey(headerTerm))
		return self.ids[headerTerm]
	
	def getTerm(self, headerTermKey):
		if headerTermKey not in self.terms:
			self.terms[headerTermKey] = VocabUtils.getTerm(headerTermKey)
		return self.terms[headerTermKey]
	
	def getAbbreviation(self, headerTermKey):
		if headerTermKey not in self.abbrev:
			self.abbrev[headerTermKey] = VocabUtils.getAbbrev(headerTermKey)
		return self.abbrev[headerTermKey]
	
class StrainGridGatherer (Gatherer.CachingMultiFileGatherer):
	# Is: a data gatherer for the strain phenotype grids and their popups.  May expand to other
	#	types of strain-specific grids in the future.
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for strains, collates results, writes
	#	tab-delimited text file
	
	def initializeCollections(self):
		# initialize the collections used by the collectData* and sort* methods
		
		# maps from (grid cell key, strain key, heading key) to a count of annotations
		self.gcCount = {}
		
		# maps from (row key, column key) to a count of annotations
		self.popCount = {}
		
		# set of all term keys seen so far
		self.termKeys = set()
		
		# set of all genotype keys seen so far
		self.genotypeKeys = set()

		# maps from header term to its computed sequence number
		# also includes mapping from (front-end header key) to the sequence number
		self.headerSeqNum = {}
		
		# maps from term key to term
		self.term = {}
		
		# maps from term to term key
		self.termKey = {}
		
		# maps from term key to its computed sequence number
		self.termKeySeqNum = {}
		
		# maps from genotype key to its computed sequence number
		self.genotypeSeqNum = {}
		return
	
	def collectDataForSlimGrids(self, strainKey, header):
		# collect data for the strain slim grid tables
		
		headingKey = HEADING_KEY_GENERATOR.getKey(header) 
		gridCellKey = GRIDCELL_KEY_GENERATOR.getKey( (strainKey, headingKey) )
		trio = (gridCellKey, strainKey, headingKey)
		
		if trio not in self.gcCount:
			self.gcCount[trio] = 0
		self.gcCount[trio] = self.gcCount[trio] + 1
		return
	
	def collectDataForPopups(self, strainKey, genotypeKey, header, termKey, term):
		# collect data for the popups that are reached by clicking a cell on a strain slim grid
		
		self.termKeys.add(termKey)
		self.genotypeKeys.add(genotypeKey)

		self.termKey[term] = termKey
		self.term[termKey] = term
		
		gridCellKey = GRIDCELL_KEY_GENERATOR.getKey( (strainKey, HEADING_KEY_GENERATOR.getKey(header)) )
		rowKey = ROW_KEY_GENERATOR.getKey( (gridCellKey, genotypeKey) )
		columnKey = COLUMN_KEY_GENERATOR.getKey( (gridCellKey, termKey) )
		pair = (rowKey, columnKey)
		
		if pair not in self.popCount:
			self.popCount[pair] = 0
		self.popCount[pair] = self.popCount[pair] + 1
		return
	
	def sortHeaders(self):
		# generate the sequence numbers for header terms
		
		headers = HEADING_KEY_GENERATOR.getDataValues()
		headers.sort(symbolsort.nomenCompare)
		i = 0
		for header in headers:
			self.headerSeqNum[header] = i
			self.headerSeqNum[HEADING_KEY_GENERATOR.getKey(header)] = i
			i = i + 1
		logger.debug('Sorted %d headers' % len(self.headerSeqNum))
		return
	
	def sortTerms(self):
		# generate the sequence numbers for annotated terms
		
		allTerms = self.termKey.keys()
		allTerms.sort(symbolsort.nomenCompare)
		i = 0
		for term in allTerms:
			self.termKeySeqNum[self.termKey[term]] = i
			i = i + 1
		logger.debug('Sorted %d terms' % len(self.termKeySeqNum))
		return
	
	def sortGenotypes(self):
		cols, rows = self.results[1]
		genotypeCol = Gatherer.columnNumber(cols, '_Genotype_key')
		allelesCol = Gatherer.columnNumber(cols, 'allele_pairs')
		
		# list of (allele pairs, genotype key) tuples to sort
		toSort = []
		
		for row in rows:
			toSort.append( (row[allelesCol], row[genotypeCol]) )
			
		toSort.sort(compareGenotypeTuple)
		i = 0
		for (allelePairs, genotypeKey) in toSort:
			self.genotypeSeqNum[genotypeKey] = i
			i = i + 1
			
		logger.debug('Sorted %d genotypes' % len(self.genotypeSeqNum))
		return
	
	def buildHeadingTables(self):
		# populates the SG_HEADING and SG_HEADING_TO_TERM tables
		
		for header in HEADING_KEY_GENERATOR.getDataValues():
			headerKey = HEADING_KEY_GENERATOR.getKey(header)	# new heading_key in front-end db
			headerTermKey = self.mpHeaderMap.getKey(header)		# existing term_key for header

			self.addRow(SG_HEADING, [ headerKey, header, self.mpHeaderMap.getAbbreviation(headerTermKey),
				'MP', 'MP', self.headerSeqNum[header] ])
			
			if headerTermKey:
				self.addRow(SG_HEADING_TO_TERM, [ headerKey, headerTermKey, self.mpHeaderMap.getID(header) ])
				
		logger.debug('Built heading tables (%d rows)' % len(HEADING_KEY_GENERATOR)) 
		return
	
	def buildGridCellTable(self):
		# populates the SG_CELL table
		
		for (trio, count) in self.gcCount.items():
			(gridCellKey, strainKey, headingKey) = trio
			color = 0
			if count > 0:
				color = 1
				
			self.addRow(SG_CELL, [ gridCellKey, strainKey, headingKey, color, count, self.headerSeqNum[headingKey] ])

		logger.debug('Built grid cell table (%d rows)' % len(self.gcCount))
		return
	
	def buildPopupTables(self):
		# populates the SG_POPUP_ROW, SG_POPUP_COLUMN, and SG_POPUP_CELL tables
		
		rowsDone = set()		# set of rowKeys we've already added records for
		columnsDone = set()		# set of columnKeys we've already added records for
		
		for (pair, count) in self.popCount.items():
			(rowKey, columnKey) = pair
			(gridCellKey, genotypeKey) = ROW_KEY_GENERATOR.getDataValue(rowKey)
			(gridCellKey, termKey) = COLUMN_KEY_GENERATOR.getDataValue(columnKey)
			
			if rowKey not in rowsDone:
				rowsDone.add(rowKey)
				if genotypeKey in self.genotypeSeqNum:
					seqNum = self.genotypeSeqNum[genotypeKey]
				else:
					seqNum = len(self.genotypeSeqNum) + genotypeKey

				self.addRow(SG_POPUP_ROW, [ rowKey, gridCellKey, genotypeKey, seqNum ])
				
			if columnKey not in columnsDone:
				columnsDone.add(columnKey)
				self.addRow(SG_POPUP_COLUMN, [ columnKey, gridCellKey, self.term[termKey], self.termKeySeqNum[termKey] ])
				
			if count > 99:
				color = 4		# 100+ annotations
			elif count > 5:
				color = 3		# 6-99
			elif count > 1:
				color = 2		# 2-5
			else:
				color = 1
				
			self.addRow(SG_POPUP_CELL, [ rowKey, columnKey, color, count, self.termKeySeqNum[termKey] ])
		
		logger.debug('Built popup tables (%d cells)' % len(self.popCount))
		return

	def collateResults(self):
		self.initializeCollections()

		cols, rows = self.results[0]
		strainCol = Gatherer.columnNumber(cols, '_Strain_key')
		genotypeCol = Gatherer.columnNumber(cols, '_Genotype_key')
		termKeyCol = Gatherer.columnNumber(cols, '_Term_key')
		termCol = Gatherer.columnNumber(cols, 'term')
		
		for row in rows:
			strainKey = row[strainCol]
			termKey = row[termKeyCol]
			
			for header in DiseasePortalUtils.getMPHeaders(termKey):
				self.collectDataForSlimGrids(strainKey, header)
				self.collectDataForPopups(strainKey, row[genotypeCol], header, termKey, row[termCol])

		logger.debug('Did basic data collation (%d rows)' % len(rows))
		
		self.sortHeaders()
		self.sortTerms()
		self.sortGenotypes()

		self.mpHeaderMap = MPHeaderMap()
		self.buildHeadingTables()
		self.buildGridCellTable()
		self.buildPopupTables()
		return

###--- globals ---###

cmds = [
	# 0. MP annotations with a null qualifier, tied to a genotype and its strain
	'''select s._Strain_key, g._Genotype_key, a._Term_key, t.term
		from %s s, gxd_genotype g, voc_annot a, voc_term t
		where s._Strain_key = g._Strain_key
			and g._Genotype_key = a._Object_key
			and a._AnnotType_key = 1002
			and a._Term_key = t._Term_key
			and a._Qualifier_key = 2181423''' % StrainUtils.getStrainTempTable(),
			
	# 1. genotype data related to our selected set of strains
	'''select g._Genotype_key, mnc.note as allele_pairs
		from %s s,
			gxd_genotype g,
			mgi_note mn,
			mgi_notechunk mnc,
			mgi_notetype mnt
		where s._Strain_key = g._Strain_key
			and g._Genotype_key = mn._Object_key
			and mn._MGIType_key = 12
			and mn._NoteType_key = mnt._NoteType_key
			and mnt.noteType = 'Combination Type 1'
			and mn._Note_key = mnc._Note_key
			and mnc.sequenceNum = 1
		order by g._Genotype_key''' % StrainUtils.getStrainTempTable(),
	]

files = [
	# The first three tables contain data for the pheno slimgrid on the strain detail page.
	
	(SG_CELL,
		[ 'grid_cell_key', 'strain_key', 'heading_key', 'color_level', 'value', 'sequence_num'],
		[ 'grid_cell_key', 'strain_key', 'heading_key', 'color_level', 'value', 'sequence_num']
		),
	(SG_HEADING,
		[ 'heading_key', 'heading', 'heading_abbreviation', 'grid_name', 'grid_name_abbreviation', 'sequence_num', ],
		[ 'heading_key', 'heading', 'heading_abbreviation', 'grid_name', 'grid_name_abbreviation', 'sequence_num', ]
		),
	(SG_HEADING_TO_TERM,
		[ 'heading_key', 'term_key', 'term_id' ],
		[ Gatherer.AUTO, 'heading_key', 'term_key', 'term_id' ]
		),
		
	# The last three tables contain data for the popups shown to a user when he clicks on a
	# pheno slimgrid row.
	
	(SG_POPUP_ROW,
		[ 'row_key', 'grid_cell_key', 'genotype_key', 'sequence_num' ],
		[ 'row_key', 'grid_cell_key', 'genotype_key', 'sequence_num' ]
		),
	(SG_POPUP_COLUMN,
		[ 'column_key', 'grid_cell_key', 'term', 'sequence_num' ],
		[ 'column_key', 'grid_cell_key', 'term', 'sequence_num' ],
		),
	(SG_POPUP_CELL,
		[ 'row_key', 'column_key', 'color_level', 'value', 'sequence_num' ],
		[ Gatherer.AUTO, 'row_key', 'column_key', 'color_level', 'value', 'sequence_num' ],
		),
	]

# global instance of a StrainGridGatherer
gatherer = StrainGridGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
