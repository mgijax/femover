#!/usr/local/bin/python
# 
# gathers data for the 'expression_result_summary' and 
# 'expression_result_to_imagepane' tables in the front-end database
#
# HISTORY
#
# 08/27/2012	lec
#	- TR11150/scrum-dog TR10273
#	add checks to gxd_expression
#	Assays that are not fully-coded will not be loaded into this gatherer
#	

import Gatherer
import logger
import symbolsort
import ReferenceCitations
import types
import VocabSorter
import GXDUtils
import OutputFile
import gc

ReferenceCitations.restrict('GXD_Assay')    # only references tied to assays
VocabSorter.setVocabs(91)		    # EMAPS

###--- Globals ---###

# list of strengths, in order of increasing strength
ORDERED_STRENGTHS = [ 'Not Applicable', 'Absent', 'Not Specified', 
	'Ambiguous', 'Trace', 'Weak', 'Present', 'Moderate', 'Strong',
	'Very strong',
	]
POSITIVE_STRENGTHS = ['Trace','Weak','Present','Moderate','Strong','Very Strong']
NEGATIVE_STRENGTHS = ['Absent']

CACHE_SIZE = OutputFile.LARGE_CACHE

# HERE:
# -----
# Already garbage collected after processing query results.  Now need to use
# factory to instantiate writers for the 5 output files, then switch to using
# them.
FILES = OutputFile.CachingOutputFileFactory()

###--- Functions ---###

def getIsExpressed(strength):
	# convert the full 'strength' value (detection level) to its
	# counterpart for the 3-valued 'is expressed?' flag

	s = strength.lower()
	if s == 'absent':
		return 'No'
	elif s in [ 'ambiguous', 'not specified' ]:
		return 'Unknown/Ambiguous'
	return 'Yes'

# list of (old, new) pairs for use in seek-and-replace loops in abbreviate()
TIMES = [ (' day ',''), ('week ','w '), ('month ','m '), ('year ','y ') ]

# list of (old, new) pairs for use in seek-and-replace loops in abbreviate()
QUALIFIERS = [ ('embryonic', 'E'), ('postnatal', 'P') ]

def abbreviate (
	s		# string; specimen's age from gxd_expression.age
	):
	# Purpose: convert 's' to a more condensed format for display on a
	#	query results page
	# Returns: string; with substitutions made as given in 'TIMES' and
	#	'QUALIFIERS' above.
	# Assumes: 's' contains at most one value from 'TIMES' and one value
	#	from 'QUALIFIERS'.  This is for efficiency, so we don't have
	#	to check every one for every invocation.
	# Effects: nothing
	# Throws: nothing

	# we have two different lists of (old, new) strings to check...
	for items in [ TIMES, QUALIFIERS ]:
		for (old, new) in items:

			# if we do not find 'old' in 's', then we just go back
			# up to continue the inner loop.

			pos = s.find(old)
			if pos == -1:
				continue

			# otherwise, we replace 'old' with 'new' and break out
			# of the inner loop to go back to the outer one.

			s = s.replace(old, new)
			break
	return s

def maxStrength (strength1, strength2):
	# combine two strengths (from gel bands for the same gel lane) to get
	# a strength for the gel lane as a whole.  At the moment, we choose
	# the stronger of the two strengths, as defined in ORDERED_STRENGTHS.
	# All strengths should appear in ORDERED_STRENGTHS.  In case new
	# strengths are added to the database, they will be sorted after ones
	# in ORDERED_STRENGTHS.  If both are not in ORDERED_STRENGTHS, then
	# they are sorted alphabetially.

	if strength1 == strength2:		# if they match, just pick one
		return strength1

	if strength1 in ORDERED_STRENGTHS:
		if strength2 in ORDERED_STRENGTHS:
			if ORDERED_STRENGTHS.index(strength1) > \
				ORDERED_STRENGTHS.index(strength2):
					return strength1
			else:
				return strength2

		else:
			return strength1

	elif strength2 in ORDERED_STRENGTHS:
		return strength2

	elif strength1 > strength2:
		return strength1

	return strength2

###--- Related functions for sorting in getSequenceNumTable method ---###

ASSAY = 0	# indices into tuples contained in the list we're sorting
SYMBOL = 1
AGE_MIN = 2
AGE_MAX = 3
STRUCTURE = 4
STAGE = 5
EXPRESSED = 6
MUTANTS = 7
REFS = 8
RESULT_KEY = 9

def _byStructureStage (a, b):
	# re-usable function to compare tuples by structure then stage

	x = cmp(a[STRUCTURE], b[STRUCTURE])
	if x == 0:
		x = cmp(a[STAGE], b[STAGE])
	return x

def _byAgeMinMax (a, b):
	# re-usable function to compare tuples by age min then age max

	x = cmp(a[AGE_MIN], b[AGE_MIN])
	if x == 0:
		x = cmp(a[AGE_MAX], b[AGE_MAX])
	return x

def byAssayType (a, b):
	# comparison function to sort by assay type (plus 7 more levels),
	# eg- (assay, symbol, ageMin, ageMax,
	#	structure, stage, expressed, resultKey)

	x = cmp(a[ASSAY], b[ASSAY])
	if x == 0:
		x = cmp(a[SYMBOL], b[SYMBOL])
		if x == 0:
			x = _byAgeMinMax(a, b)
			if x == 0:
				x = _byStructureStage(a, b)
				if x == 0:
					x = cmp(a[EXPRESSED], b[EXPRESSED])
					if x == 0:
						x = cmp(a[RESULT_KEY],
							b[RESULT_KEY])
	return x

def bySymbol (a, b):
	# comparison function to sort by symbol (plus 7 more levels),
	# eg- (symbol, assay, ageMin, ageMax,
	#	structure, stage, expressed, resultKey)

	x = cmp(a[SYMBOL], b[SYMBOL])
	if x == 0:
		x = cmp(a[ASSAY], b[ASSAY])
		if x == 0:
			x = _byAgeMinMax(a, b)
			if x == 0:
				x = _byStructureStage(a, b)
				if x == 0:
					x = cmp(a[EXPRESSED], b[EXPRESSED])
					if x == 0:
						x = cmp(a[RESULT_KEY],
							b[RESULT_KEY])
	return x

def byAge (a, b):
	# comparison function to sort by age (plus 7 more levels),
	# eg- (ageMin, ageMax, structure, stage, 
	#	expressed, symbol, assay, resultKey)

	x = _byAgeMinMax(a, b)
	if x == 0:
		x = _byStructureStage(a, b)
		if x == 0:
			x = cmp(a[EXPRESSED], b[EXPRESSED])
			if x == 0:
				x = cmp(a[SYMBOL], b[SYMBOL])
				if x == 0:
					x = cmp(a[ASSAY], b[ASSAY])
					if x == 0:
						x = cmp(a[RESULT_KEY],
							b[RESULT_KEY])
	return x

def byStructure (a, b):
	# comparison function to sort by age (plus 7 more levels),
	# eg- (structure, stage, ageMin, ageMax,
	#	expressed, symbol, assay, resultKey)

	x = _byStructureStage(a, b)
	if x == 0:
		x = _byAgeMinMax(a, b)
		if x == 0:
			x = cmp(a[EXPRESSED], b[EXPRESSED])
			if x == 0:
				x = cmp(a[SYMBOL], b[SYMBOL])
				if x == 0:
					x = cmp(a[ASSAY], b[ASSAY])
					if x == 0:
						x = cmp(a[RESULT_KEY],
							b[RESULT_KEY])
	return x

def byExpressed (a, b):
	# comparison function to sort by whether expression was shown (plus 7)
	# eg- expressed, symbol, assay, ageMin, 
	#	ageMax, structure, stage, resultKey)

	x = cmp(a[EXPRESSED], b[EXPRESSED])
	if x == 0:
		x = cmp(a[SYMBOL], b[SYMBOL])
		if x == 0:
			x = cmp(a[ASSAY], b[ASSAY])
			if x == 0:
				x = _byAgeMinMax(a, b)
				if x == 0:
					x = _byStructureStage(a, b)
					if x == 0:
						x = cmp(a[RESULT_KEY],
							b[RESULT_KEY])
	return x

def byMutants (a, b):
	# comparison function to sort by mutant alleles (plus 7 more levels),
	# eg- (mutants, symbol, assay, ageMin,
	#	ageMax, structure, stage, resultKey)

	x = cmp(a[MUTANTS], b[MUTANTS])
	if x == 0:
		x = cmp(a[SYMBOL], b[SYMBOL])
		if x == 0:
			x = cmp(a[ASSAY], b[ASSAY])
			if x == 0:
				x = _byAgeMinMax(a, b)
				if x == 0:
					x = _byStructureStage(a, b)
					if x == 0:
						x = cmp(a[RESULT_KEY],
							b[RESULT_KEY])
	return x

def byReference (a, b):
	# comparison function to sort by reference (plus 7 more levels),
	# eg- (refs, symbol, assay, ageMin, ageMax,
	#	structure, stage, resultKey) )

	x = cmp(a[REFS], b[REFS])
	if x == 0:
		x = cmp(a[SYMBOL], b[SYMBOL])
		if x == 0:
			x = cmp(a[ASSAY], b[ASSAY])
			if x == 0:
				x = _byAgeMinMax(a, b)
				if x == 0:
					x = _byStructureStage(a, b)
					if x == 0:
						x = cmp(a[RESULT_KEY],
							b[RESULT_KEY])
	return x

###--- Classes ---###

class ExpressionResultSummaryGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the expression_result_summary and
	#	expression_result_to_imagepane tables
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for expression
	#	results and imagepanes, collates data, writes tab-delimited
	#	text files

	# a lookup of the format {marker_key=>{structure_key => [allCount,detectedCount,notDetectedCount]}}
	# for building the marker_tissue_expression_counts table
	markerTissueCounts = {}

	def addMarkerTissueCount(self,marker_key,emapsKey,stage, structure, detectionLevel):
		structures = self.markerTissueCounts.setdefault(marker_key,{})
		# counts format is [allCount,detectedCount,notDetectedCount]
		countsInfo = structures.setdefault(emapsKey,{'stage': stage, 
												'structure': structure,
												'counts':[0,0,0]})
		
		counts = countsInfo['counts']
		
		if detectionLevel in NEGATIVE_STRENGTHS:
			# negative case
			counts[0] += 1
			counts[2] += 1
		elif detectionLevel in POSITIVE_STRENGTHS:
			# positive case
			counts[0] += 1 
			counts[1] += 1
		else:
			# ambiguous case
			counts[0] += 1

	def getAssayIDs(self):
		# handle query 0 : returns { assay key : MGI ID }
		ids = {}

		cols, rows = self.results[0]

		idCol = Gatherer.columnNumber (cols, 'accID')
		keyCol = Gatherer.columnNumber (cols, '_Assay_key')

		for row in rows:
			ids[row[keyCol]] = row[idCol]

		logger.debug ('Got %d assay IDs' % len(ids))

		# reclaim memory once we have processed the results
		self.results[0] = [ cols, [] ]
		gc.collect()

		return ids

	def getJnumbers(self):
		# handle query 1 : returns { refs key : jnum ID }
		jnum = {}

		cols, rows = self.results[1]

		idCol = Gatherer.columnNumber (cols, 'accID')
		keyCol = Gatherer.columnNumber (cols, '_Refs_key')

		for row in rows:
			jnum[row[keyCol]] = row[idCol]

		logger.debug ('Got %d Jnum IDs' % len(jnum))

		# reclaim memory once we have processed the results
		self.results[1] = [ cols, [] ]
		gc.collect()

		return jnum

	def getMarkerSymbols(self):
		# handle query 2 : returns { marker key : marker symbol }
		symbol = {}

		cols, rows = self.results[2]

		symbolCol = Gatherer.columnNumber (cols, 'symbol')
		keyCol = Gatherer.columnNumber (cols, '_Marker_key')

		allSymbols = []

		for row in rows:
			symbol[row[keyCol]] = row[symbolCol]
			allSymbols.append (row[symbolCol])

		logger.debug ('Got %d marker symbols' % len(symbol))

		allSymbols.sort (symbolsort.nomenCompare)

		self.symbolSequenceNum = {}
		i = 0
		for sym in allSymbols:
			i = i + 1
			self.symbolSequenceNum[sym] = i

		logger.debug ('Sorted %d marker symbols' % i) 

		# reclaim memory once we have processed the results
		del allSymbols
		self.results[2] = [ cols, [] ]
		gc.collect()

		return symbol

	def getDisplayableImagePanes(self):
		# handle query 3 : returns { image pane key : 1 }
		panes = {}

		cols, rows = self.results[3]

		for row in rows:
			panes[row[0]] = 1

		logger.debug ('Got %d displayable image panes' % len(panes))

		# reclaim memory once we have processed the results
		self.results[3] = [ cols, [] ]
		gc.collect()

		return panes

	def getPanesForInSituResults(self):
		# handle query 4 : returns { assay key : [ image pane keys ] }
		panes = {}

		cols, rows = self.results[4]

		assayCol = Gatherer.columnNumber (cols, '_Result_key')
		paneCol = Gatherer.columnNumber (cols, '_ImagePane_key')

		for row in rows:
			assay = row[assayCol]
			if panes.has_key(assay):
				panes[assay].append (row[paneCol])
			else:
				panes[assay] = [ row[paneCol] ]

		logger.debug ('Got panes for %d in situ assays' % len(panes))

		# reclaim memory once we have processed the results
		self.results[4] = [ cols, [] ]
		gc.collect()

		return panes

	def getBasicAssayData(self):
		# handle query 5 : returns [ [ assay key, assay type,
		#	reference key, marker key, image pane key,
		#	reporter gene key, is gel flag ], ... ]
		assays = []

		cols, rows = self.results[5]

		assayCol = Gatherer.columnNumber (cols, '_Assay_key')
		assayTypeCol = Gatherer.columnNumber (cols, 'assayType')
		paneCol = Gatherer.columnNumber (cols, '_ImagePane_key')
		refsCol = Gatherer.columnNumber (cols, '_Refs_key')
		markerCol = Gatherer.columnNumber (cols, '_Marker_key')
		reporterCol = Gatherer.columnNumber (cols,'_ReporterGene_key')
		isGelCol = Gatherer.columnNumber (cols, 'isGelAssay')

		for row in rows:
			assays.append ( [ row[assayCol], row[assayTypeCol],
				row[refsCol], row[markerCol], row[paneCol],
				row[reporterCol], row[isGelCol] ] )

		logger.debug ('Got basic data for %d assays' % len(assays))

		# reclaim memory once we have processed the results
		self.results[5] = [ cols, [] ]
		gc.collect()

		return assays

	def getInSituExtraData(self):
		# handle query 6 : returns { assay key : [ genotype key,
		#	age, strength, result key, structure key ] }
		extras = {}

		cols, rows = self.results[6]

		assayCol = Gatherer.columnNumber (cols, '_Assay_key')
		genotypeCol = Gatherer.columnNumber (cols, '_Genotype_key')
		ageCol = Gatherer.columnNumber (cols, 'age')
		strengthCol = Gatherer.columnNumber (cols, 'strength')
		resultCol = Gatherer.columnNumber (cols, '_Result_key')
		emapsKeyCol = Gatherer.columnNumber (cols,'_emaps_key')
		stageCol = Gatherer.columnNumber (cols,'_stage_key')
		structureCol = Gatherer.columnNumber (cols,'structure')
		ageMinCol = Gatherer.columnNumber (cols, 'ageMin')
		ageMaxCol = Gatherer.columnNumber (cols, 'ageMax')
		patternCol = Gatherer.columnNumber (cols, 'pattern')
		specimenKeyCol = Gatherer.columnNumber (cols, '_specimen_key')

		for row in rows:
			assayKey = row[assayCol]

			emapsKey = row[emapsKeyCol]
			if not emapsKey:
				continue

			extras.setdefault(assayKey,[]).append( [row[genotypeCol],
				row[ageCol], row[strengthCol], row[resultCol],
				emapsKey, row[ageMinCol],
				row[ageMaxCol], row[patternCol],row[specimenKeyCol],
				row[stageCol], row[structureCol] ])

		logger.debug ('Got extra data for %d in situ assays' % \
			len(extras))

		# reclaim memory once we have processed the results
		self.results[6] = [ cols, [] ]
		gc.collect()

		return extras

	def getGelExtraData(self):
		# handle query 7 : returns { assay key : [ genotype key,
		#	age, strength, structure key ] }
		extras = {}

		cols, rows = self.results[7]

		assayCol = Gatherer.columnNumber (cols, '_Assay_key')
		genotypeCol = Gatherer.columnNumber (cols, '_Genotype_key')
		ageCol = Gatherer.columnNumber (cols, 'age')
		strengthCol = Gatherer.columnNumber (cols, 'strength')
		emapsKeyCol = Gatherer.columnNumber (cols,'_emaps_key')
		stageCol = Gatherer.columnNumber (cols,'_stage_key')
		structureCol = Gatherer.columnNumber (cols,'structure')
		ageMinCol = Gatherer.columnNumber (cols, 'ageMin')
		ageMaxCol = Gatherer.columnNumber (cols, 'ageMax')
		gelLaneCol = Gatherer.columnNumber (cols, '_GelLane_key')

		# as a pre-processing step, compute the strength for each
		# gel lane / structure pair

		strengths = {}		# gel lane, structure -> strength

		for row in rows:
			gelLane = row[gelLaneCol]
			emapsKey = row[emapsKeyCol]

			if not emapsKey:
				continue

			strength = row[strengthCol]

			pair = (gelLane, emapsKey)

			if not strengths.has_key(pair):
				strengths[pair] = strength
			else:
				strengths[pair] = maxStrength (strength,
					strengths[pair])

		logger.debug ('Computed strengths for %d gel results' % \
			len(strengths))

		# Now, we only want to add rows to each assay for each 
		# gel lane / structure pair.  For each pair, we pull the
		# strength from the dictionary we just pre-computed.

		seen = {}		# gel lane, structure -> 1

		for row in rows:
		    assayKey = row[assayCol]
		    gelLane = row[gelLaneCol]
		    emapsKey = row[emapsKeyCol]

		    if not emapsKey:
			    continue

		    pair = (gelLane, emapsKey)
		    strength = strengths[pair]

		    # if we have already added a row for this pair, then we
		    # can move on to the next one

		    if seen.has_key(pair):
			    continue

		    seen[pair] = 1

		    if extras.has_key(assayKey):
			extras[row[assayCol]].append( [ row[genotypeCol],
				row[ageCol], strength,
				emapsKey, row[ageMinCol],
				row[ageMaxCol],
				row[stageCol], row[structureCol] ] )
		    else:
			extras[row[assayCol]] = [ [ row[genotypeCol],
				row[ageCol], strength,
				emapsKey, row[ageMinCol],
				row[ageMaxCol],
				row[stageCol], row[structureCol] ] ]

		logger.debug ('Got extra data for %d gel assays' % \
			len(extras))

		# reclaim memory once we have processed the results
		self.results[7] = [ cols, [] ]
		gc.collect()

		return extras

	def sortGenotypeData(self):
		cols, rows = self.results[8]

		genotypeCol = Gatherer.columnNumber (cols, '_Genotype_key')
		allele1Col = Gatherer.columnNumber (cols, 'symbol1')
		allele2Col = Gatherer.columnNumber (cols, 'symbol2')

		byGenotype = {}

		for row in rows:
			genotypeKey = row[genotypeCol]
			allele1 = row[allele1Col]
			allele2 = row[allele2Col]

			if not byGenotype.has_key(genotypeKey):
				byGenotype[genotypeKey] = [ allele1, allele2 ]
			else:
				byGenotype[genotypeKey].append (allele1)
				byGenotype[genotypeKey].append (allele2)

		toSort = []
		for (key, alleles) in byGenotype.items():
			toSort.append ( (alleles, key) )

		def custom (a, b):
			# comparator to sort based on strings of allele
			# symbols, such that a[i] is compared to b[i] for each
			# value of i up to minimum length of a,b

			max_i = min(len(a[0]), len(b[0]))

			i = 0
			while i < max_i:
				if a[0][i] == None:
					return -1
				elif b[0][i] == None:
					return 1

				c = symbolsort.nomenCompare (a[0][i], b[0][i])
				if c != 0:
					return c
				i = i + 1

			if len(a[0]) > len(b[0]):
				return 1
			elif len(b[0]) > len(a[0]):
				return -1
			return 0

		toSort.sort(custom)

		self.byGenotype = {}
		i = 0

		for (alleles, key) in toSort:
			i = i + 1
			self.byGenotype[key] = i

		logger.debug ('Sorted %d genotypes by allele pairs' % i)

		# reclaim memory once we have processed the results
		self.results[8] = [ cols, [] ]
		gc.collect()

		return

	def getWildTypeAssays (self):
		# get a dictionary with two types of keys
		#	genotype key : where the genotype is always wild-type
		#	(genotype key, assay key) : where the genotype is
		#		sometimes wild-type, depending partly on the
		#		type of assay with which it is associated

		wildtype = {}

		# query 9 - genotypes that are always wild-type

		cols, rows = self.results[9]

		for row in rows:
			wildtype[row[0]] = 1

		logger.debug ('Found %d wild-type genotypes' % len(rows))

		# reclaim memory once we have processed the results
		self.results[9] = [ cols, [] ]
		gc.collect()

		# query 10 - genotype/assay pairs that are wild-type together

		cols, rows = self.results[10]

		genotypeCol = Gatherer.columnNumber (cols, '_Genotype_key')
		assayCol = Gatherer.columnNumber (cols, '_Assay_key')

		for row in rows:
			wildtype[(row[genotypeCol], row[assayCol])] = 1

		logger.debug ('Found %d wild-type genotype/assay pairs' % \
			len(rows))

		# reclaim memory once we have processed the results
		self.results[10] = [ cols, [] ]
		gc.collect()

		return wildtype

	def collateResults (self):

		# get caches of data

		assayIDs = self.getAssayIDs()
		jNumbers = self.getJnumbers()
		symbols = self.getMarkerSymbols()
		displayablePanes = self.getDisplayableImagePanes()
		panesForInSituResults = self.getPanesForInSituResults()

		assays = self.getBasicAssayData()
		inSituExtras = self.getInSituExtraData()
		gelExtras = self.getGelExtraData()

		wildtype = self.getWildTypeAssays()

		# definitions for expression_result_summary table data

		ersCols = [ 'result_key', '_Assay_key', 'assayType',
			'assayID', '_Marker_key', 'symbol', 'stage',
			'age', 'ageAbbreviation', 'age_min', 'age_max',
			'structure', 'printname', 'structureKey',
			'detectionLevel', 'isExpressed', '_Refs_key',
			'jnumID', 'hasImage', '_Genotype_key', 'is_wild_type',
			'pattern','_Specimen_key'
			]

		ersFile = FILES.createFile(files[0][0],
			ersCols, files[0][1], CACHE_SIZE)

		# excerpt of summary data that will be used for sorting

		sortCols = [ 'result_key', '_Assay_key', 'assayType',
			'symbol', 'stage', 'structureKey',
			'isExpressed', '_Refs_key', '_Genotype_key',
			]
		sortRows = []

		# definitions for expression_result_to_imagepane table data

		ertiCols = [ 'result_key', '_ImagePane_key',
			'sequence_num' ]

		ertiCount = 0
		ertiFile = FILES.createFile(files[1][0], ertiCols,
			files[1][1], CACHE_SIZE)

		ageMinMax = {}		# result key -> (age min, age max)

		# definitions for expression_result_anatomical_systems
		# table data

		erasCols =  [ 'result_key', 'emapa_id', 'anatomical_structure' ]
		erasFile = FILES.createFile(files[4][0], erasCols,
			files[4][1], CACHE_SIZE)

		# now, we need to walk through our assays to populate the list
		# of data for each table

		newKey = 0		# unique key for each result

		for [ assayKey, assayType, refsKey, markerKey, imagepaneKey,
		    reporterGeneKey, isGel ] in assays:
			
		    #
		    # select the 'extra' assay information
		    #

		    extras = []
		    if isGel:
			if gelExtras.has_key(assayKey):
			    extras = gelExtras[assayKey]
		    else:
			if inSituExtras.has_key(assayKey):
			    extras = inSituExtras[assayKey]

		    for items in extras:

			hasImage = 0	# is there a displayable image for
					# ...this result?

			panes = []	# image panes for this result

			if isGel:
			    [ genotypeKey, age, strength,
				emapsKey, ageMin, ageMax,
				stage, structure ] = items

			    pattern = None
			    specimenKey = None

			    if imagepaneKey:
				panes = [ imagepaneKey ]

				if displayablePanes.has_key(imagepaneKey):
				    hasImage = 1
			else:
			    [ genotypeKey, age, strength, resultKey,
				emapsKey, ageMin, ageMax,
				pattern,specimenKey,
				stage, structure ] = items

			    if panesForInSituResults.has_key(resultKey):
				panes = panesForInSituResults[resultKey]

				for paneKey in panes:
				    if displayablePanes.has_key(paneKey):
					hasImage = 1
					break

		    	if wildtype.has_key(genotypeKey):
				isWildType = 1
		    	elif wildtype.has_key( (genotypeKey, assayKey) ):
				isWildType = 1
			else:
		    		isWildType = 0

			# while we are in here, add count statistics of tissues
			self.addMarkerTissueCount(markerKey,emapsKey,stage, structure, strength)

			newKey = newKey + 1

			outRow = [ newKey,
			    assayKey,
			    assayType,
			    assayIDs[assayKey],
			    markerKey,
			    symbols[markerKey],
			    stage,
			    age,
			    abbreviate(age),
			    ageMin,
			    ageMax,
			    structure,
			    structure,
			    emapsKey,
			    strength,
			    getIsExpressed(strength),
			    refsKey,
			    jNumbers[refsKey],
			    hasImage,
			    genotypeKey,
			    isWildType,
			    pattern,
			    specimenKey
			    ]

			FILES.addRow(ersFile, outRow)

			sortRow = [ newKey,
			    assayKey,
			    assayType,
			    symbols[markerKey],
			    stage,
			    emapsKey,
			    getIsExpressed(strength),
			    refsKey,
			    genotypeKey,
			    ]

			sortRows.append (sortRow)

			ageMinMax[newKey] = (ageMin, ageMax)

			for paneKey in panes:
			    ertiCount = ertiCount + 1
			    paneRow = [ newKey, paneKey, ertiCount ]
			    FILES.addRow(ertiFile, paneRow)

			# include anatomical structures for the result -- used
			# for filtering in the fewi

			emapaKey = GXDUtils.getEmapaKey(emapsKey)
			highLevelTerms = GXDUtils.getEmapaHighLevelTerms(
				emapaKey, stage)

			for (accID, structure) in highLevelTerms:
				FILES.addRow(erasFile,
					[ newKey, accID, structure ] )

		logger.debug ('Got %d GXD result summary rows' % \
			FILES.getRowCount(ersFile))
		logger.debug ('Got %d GXD result/pane rows' % \
			FILES.getRowCount(ertiFile))

		del assays
		GXDUtils.unload()

		# process markerTissueCounts data

		mtCols = [ '_Marker_key', '_Structure_key',
                        'printName', 'allCount', 'detectedCount',
                        'notDetectedCount', 'sequenceNum' ]

		self.processMarkerTissueCounts(mtCols,
			self.markerTissueCounts)
		
		self.markerTissueCounts = {}	# free up memory...
		gc.collect()

		logger.debug('Got %d high-level EMAPA terms for %d results' \
			% (FILES.getRowCount(erasFile),
				FILES.getRowCount(ersFile)) )

		# build the sequence number table using data from loop above

		self.getSequenceNumTable (sortCols, sortRows, ageMinMax)

		# We don't need the sortRows anymore, so free up the memory.

		del sortRows
		gc.collect()

		# ensure that everything is written out to disk, then report
		# which data files we produced and where they are

		FILES.closeAll()
		FILES.reportAll()
		return

	def processMarkerTissueCounts(self, mtCols, markerTissueCounts):
		mtFile = FILES.createFile (files[3][0], mtCols, files[3][1],
			CACHE_SIZE)

		for markerKey,structures in markerTissueCounts.items():
			for emapsKey,countInfo in structures.items():
				seqNum = VocabSorter.getSequenceNum(emapsKey)
				
				counts = countInfo['counts']
				stage = countInfo['stage']
				structure = countInfo['structure']

				FILES.addRow(mtFile, [ markerKey, emapsKey,
					"TS%s: %s" % (stage, structure),
					counts[0], counts[1], counts[2],
					seqNum ] )

		logger.debug('Got %d marker/tissue counts' % \
			FILES.getRowCount(mtFile))
		return

	def getSymbolSequenceNum (self, symbol):
		if self.symbolSequenceNum.has_key(symbol):
			return self.symbolSequenceNum[symbol]
		return len(self.symbolSequenceNum) + 1

	def getAssayTypeSequenceNum (self, assayType):
		if assayType == 'Immunohistochemistry':
			return 1
		elif assayType == 'RNA in situ':
			return 2
		elif assayType == 'In situ reporter (knock in)':
			return 3
		elif assayType == 'Northern blot':
			return 4
		elif assayType == 'Western blot':
			return 5
		elif assayType == 'RT-PCR':
			return 6
		elif assayType == 'RNase protection':
			return 7
		elif assayType == 'Nuclease S1':
			return 8
		return 9

	def getStageSequenceNum (self, stage):
		return int(stage)

	def getExpressedSequenceNum (self, isExpressed):
		if isExpressed == 'Yes':
			return 1
		elif isExpressed == 'No':
			return 2
		return 3

	def getGenotypeSequenceNum (self, genotypeKey):
		if self.byGenotype.has_key(genotypeKey):
			return self.byGenotype[genotypeKey]
		return len(self.byGenotype) + 1

	def getSequenceNumTable (self, sortCols, sortRows, ageMinMax):
		# build and return the rows (and columns) for the
		# expression_result_sequence_num table

		self.sortGenotypeData()

		cols = [ 'result_key', 'by_assay_type', 'by_gene_symbol',
			'by_age', 'by_strucure',
			'by_expressed', 'by_mutant_alleles', 'by_reference' ]

		ersnFile = FILES.createFile (files[2][0], cols, files[2][1],
			CACHE_SIZE)

		resultKeys = []		# all result_key values

		# We need to generate one row for each incoming row from 
		# 'sortRows'.  Each of these rows will have multiple pre-
		# computed sorts (as in 'cols'), and each of those needs to be
		# pre-computed as a multi-level sort.

		# In order to (dramatically) save memory, we will use a single
		# list with rows corresponding to those in 'sortRows'.  We
		# will sort the list one way, record the order, then move on
		# to re-sort the list a different way.

		rows = []	# the list we will use for re-sorting

		# fill in sortable data for each of the ordering lists
		# (right now we only do byStructure)

		resultKeyCol = Gatherer.columnNumber (sortCols, 'result_key')
		assayKeyCol = Gatherer.columnNumber (sortCols, '_Assay_key')
		assayTypeCol = Gatherer.columnNumber (sortCols, 'assayType')
		symbolCol = Gatherer.columnNumber (sortCols, 'symbol')
		stageCol = Gatherer.columnNumber (sortCols, 'stage')
		expressedCol = Gatherer.columnNumber (sortCols, 'isExpressed')
		refsKeyCol = Gatherer.columnNumber (sortCols, '_Refs_key')
		structureKeyCol = Gatherer.columnNumber (sortCols,
			'structureKey')
		genotypeKeyCol = Gatherer.columnNumber (sortCols,
			'_Genotype_key')

		logger.debug ('identified columns for sorts')

		# Previously this section would compile 7 separate ordering
		# lists, then sort them, then collate them.  This uses 7x the
		# memory of doing the 3 steps for each in succession.

		for row in sortRows:
		    resultKey = row[resultKeyCol]

		    emapsKey = row[structureKeyCol]
		    structure = VocabSorter.getSequenceNum(emapsKey)
		    stageVal = row[stageCol]

		    if (not emapsKey) or (not stageVal):
			    continue

		    stage = self.getStageSequenceNum (stageVal)
		    ageMin = ageMinMax[resultKey][0]
		    ageMax = ageMinMax[resultKey][1]
		    expressed = self.getExpressedSequenceNum (row[expressedCol])
		    symbol = self.getSymbolSequenceNum (row[symbolCol])
		    assay = VocabSorter.getAssayTypeSequenceNum (
			row[assayTypeCol])
		    mutants = self.getGenotypeSequenceNum (row[genotypeKeyCol])
		    refs = ReferenceCitations.getSequenceNum (row[refsKeyCol])

		    # add record to the ordering list

		    rows.append ( (assay, symbol, ageMin, ageMax, structure,
			    stage, expressed, mutants, refs, resultKey) )

		    resultKeys.append (resultKey)

		logger.debug ('compiled ordering list')

		# produce the individual output row for each result key

		output = {}		# result key -> [ sort 1, ... n ]

		for key in resultKeys:
			output[key] = [ key ]

		orders = [ ('assayType', byAssayType), ('symbol', bySymbol),
			('age', byAge), ('structure', byStructure),
			('expressed', byExpressed), ('mutants', byMutants),
			('reference', byReference) ]

		for (name, comparison) in orders:
			rows.sort(comparison)
			logger.debug('Sorted %d rows by %s' % (len(rows),
				name) )

			i = 0

			# remember which result keys were not seen for
			# the rows so far

			notDone = {}
			for key in resultKeys:
				notDone[key] = 1

			# order those we know about in 'orderingList'

			for row in rows:
				i = i + 1
				output[row[-1]].append(i)
				if notDone.has_key(row[-1]):
					del notDone[row[-1]]

			# order those left over in 'notDone'

			keys = notDone.keys()
			keys.sort()

			for key in keys:
				i = i + 1
				output[row[-1]].append(i)

			logger.debug ('Finished ordering by %s' % name)

		# extract the individual rows and write them out

		keys = output.keys()
		keys.sort()

		for key in keys:
			FILES.addRow (ersnFile, output[key])

		logger.debug ('produced %d ordering rows' % \
			FILES.getRowCount(ersnFile) )
		return

###--- globals ---###

cmds = [
	# While it would be nice to make use of the gxd_expression table to
	# assemble our data, it does not provide access to some information we
	# need, like the raw 'strength' of detection values.  So, we are left
	# building the summary data back up from the base tables.

	#
	# TR10273/notes:
	# The gxd_expression cache could be changed/added to in order
	# to provide the information required by this gatherer.
	#

	# 0. MGI IDs for expression assays
	'''select _Object_key as _Assay_key,
		accID
	from acc_accession
	where _MGIType_key = 8
		and _LogicalDB_key = 1
		and private = 0
		and preferred = 1''',

	# 1. J: numbers (IDs) for references with expression data
	'''select a._Object_key as _Refs_key,
		a.accID
	from acc_accession a
	where a._MGIType_key = 1
		and a.private = 0
		and a.preferred = 1
		and a._LogicalDB_key = 1
		and a.prefixPart = 'J:'
		and exists (select 1 from gxd_expression g
			where a._Object_key = g._Refs_key)''',

	# 2. marker symbols studied in expression data
	'''select m._Marker_key, m.symbol
	from mrk_marker m
	where exists (select 1 from gxd_expression g
		where m._Marker_key = g._Marker_key)''',

	# 3. list of image panes which have dimensions (indicates whether we
	# have the actual image available for display or not)

	'''select p._ImagePane_key
	from img_imagePane p, img_image i, voc_term t
	where p._Image_key = i._Image_key
		and i.xDim is not null
		and i._ImageClass_key = t._Term_key
		and t.term = 'Expression' ''',

	# 4. image panes for in situ results
	'''select distinct i._Result_key, i._ImagePane_key
	from gxd_insituresultimage i''',

	# 5. basic assay data (image key here is for gel assays)
	'''select a._Assay_key, a._AssayType_key, t.assayType,
		a._Refs_key, a._Marker_key, a._ImagePane_key,
		a._ReporterGene_key, t.isGelAssay
	from gxd_assay a, gxd_assaytype t
	where a._AssayType_key = t._AssayType_key
	and exists (select 1 from gxd_expression e where e._Assay_key = a._Assay_key and e.isForGxd = 1)''',

	# 6. additional data for in situ assays (note that there can be > 1
	# structures per result key)

	'''select s._Assay_key,
		s._Genotype_key,
		s.age,
		r._Strength_key,
		st.strength,
		r._Result_key,
		vte._term_key as _emaps_key,
		rs._stage_key,
		struct.term as structure,
		s.ageMin,
		s.ageMax,
		p.pattern,
		s._specimen_key
	from gxd_specimen s,
		gxd_insituresult r,
		gxd_strength st,
		gxd_isresultstructure rs,
		gxd_pattern p,
		voc_term_emaps vte,
		voc_term struct
	where s._Specimen_key = r._Specimen_key
		and r._Strength_key = st._Strength_key
		and r._Pattern_key = p._Pattern_key
		and r._Result_key = rs._Result_key
		and vte._emapa_term_key = rs._emapa_term_key
		and vte._stage_key = rs._stage_key
		and struct._term_key = vte._term_key
	''', 

	# 7. additional data for gel assays (skip control lanes)  (note that
	# there can be > 1 structures per gel lane).  A gel assay may have
	# multiple gel lanes.  Each lane can have multiple structures, and
	# each lane/structure pair defines an expression result (for the
	# purposes of GXD).  Each lane can have multiple bands, but those are
	# not defined as being separate results; we will need to consolidate
	# the strengths for the given bands to come up with a strength for
	# the lane as a whole.

	'''select g._Assay_key,
		g._Genotype_key,
		g.age,
		st._Strength_key,
		st.strength,
		vte._term_key as _emaps_key,
		gs._stage_key,
		struct.term as structure
		g.ageMin,
		g.ageMax,
		g._GelLane_key
	from gxd_gellane g,
		gxd_gelband b,
		gxd_strength st,
		gxd_gellanestructure gs,
		voc_term_emaps vte,
		voc_term struct
	where g._GelControl_key = 1
		and g._GelLane_key = b._GelLane_key
		and b._Strength_key = st._Strength_key
		and g._GelLane_key = gs._GelLane_key
		and vte._emapa_term_key = gs._emapa_term_key
		and vte._stage_key = gs._stage_key
		and struct._term_key = vte._term_key
	''',

	# 8. allele pairs for genotypes cited in GXD data
	'''select gap._Genotype_key,
		a1.symbol as symbol1,
		a2.symbol as symbol2
	from gxd_allelepair gap
	inner join all_allele a1 on (gap._Allele_key_1 = a1._Allele_key)
	left outer join all_allele a2 on (gap._Allele_key_2 = a2._Allele_key)
	where exists (select 1 from gxd_specimen s
			where s._Genotype_key = gap._Genotype_key)
		or exists (select 1 from gxd_gellane g
			where g._Genotype_key = gap._Genotype_key)
	order by gap.sequenceNum''',

	# 9. genotypes with no allele pairs (treat expression assays for
	# these as wild type)
	'''select g._Genotype_key
	from gxd_genotype g
	where g._Genotype_key >= 0
		and not exists (select 1 from gxd_allelepair p
			where g._Genotype_key = p._Genotype_key)''',

	# 10. also treat expression assays for these genotypes as wild type;
	# these have:
	#	a. assay type = In situ reporter (knock in) (key 9)
	#	b. only one allele pair
	#	c. allele pair with one wild type allele and one other
	#	d. both alleles for the assayed gene
	'''select distinct s._Assay_key,
		s._Genotype_key
	from gxd_specimen s,
		gxd_assay a,
		gxd_allelegenotype gag1,
		gxd_allelegenotype gag2,
		all_allele a1,
		all_allele a2
	where s._Assay_key = a._Assay_key
		and a._AssayType_key = 9
		and s._Genotype_key >= 0
		and exists (select 1 from gxd_expression e where e._Assay_key = a._Assay_key and e.isForGxd = 1)
		and not exists (select 1 from gxd_allelepair gap
			where s._Genotype_key = gap._Genotype_key
			and gap.sequenceNum > 1)
		and s._Genotype_key = gag1._Genotype_key
		and gag1._Allele_key = a1._Allele_key
		and a1.isWildType = 1
		and s._Genotype_key = gag2._Genotype_key
		and gag2._Allele_key = a2._Allele_key
		and a2.isWildType = 0
		and a1._Marker_key = a._Marker_key
		and a2._Marker_key = a._Marker_key''',
	]

files = [
	('expression_result_summary',
		[ 'result_key', '_Assay_key', 'assayType', 'assayID',
		'_Marker_key', 'symbol', 'stage', 'age',
		'ageAbbreviation', 'age_min', 'age_max',
		'structure', 'printname', 'structureKey', 'detectionLevel',
		'isExpressed', '_Refs_key', 'jnumID', 'hasImage',
		'_Genotype_key', 'is_wild_type', 'pattern' , '_Specimen_key'],
		'expression_result_summary'),

	('expression_result_to_imagepane',
		[ Gatherer.AUTO, 'result_key', '_ImagePane_key',
		'sequence_num' ],
		'expression_result_to_imagepane'),

	('expression_result_sequence_num',
		[ 'result_key', 'by_assay_type', 'by_gene_symbol',
		'by_age', 'by_strucure',
		'by_expressed', 'by_mutant_alleles', 'by_reference' ],
		'expression_result_sequence_num'),

	('marker_tissue_expression_counts',	
	       	[Gatherer.AUTO, '_Marker_key', '_Structure_key', 'printName',
		'allCount', 'detectedCount', 'notDetectedCount', 'sequenceNum'],
		'marker_tissue_expression_counts'),

	('expression_result_anatomical_systems',
		[ Gatherer.AUTO, 'result_key', 'emapa_id',
		'anatomical_structure' ],
		'expression_result_anatomical_systems'),
	]

# global instance of a ExpressionResultSummaryGatherer
gatherer = ExpressionResultSummaryGatherer (files, cmds)
gatherer.customWrites = True

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
