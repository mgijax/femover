#!/usr/local/bin/python
# 
# gathers data for the 'expression_result_summary' and 
# 'expression_result_to_imagepane' tables in the front-end database
#
# HISTORY
#
# 08/27/2012	lec
#	- TR11150/scrum-dog TR10273
#	add checks to GXD_Expression
#	Assays that are not fully-coded will not be loaded into this gatherer
#	

import Gatherer
import logger
import ADVocab
import symbolsort
import ReferenceCitations
import types
import VocabSorter

###--- Globals ---###

# list of strengths, in order of increasing strength
ORDERED_STRENGTHS = [ 'Not Applicable', 'Absent', 'Not Specified', 
	'Ambiguous', 'Trace', 'Weak', 'Present', 'Moderate', 'Strong',
	'Very strong',
	]

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
	s		# string; specimen's age from GXD_Expression.age
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

###--- Classes ---###

class ExpressionResultSummaryGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the expression_result_summary and
	#	expression_result_to_imagepane tables
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for expression
	#	results and imagepanes, collates data, writes tab-delimited
	#	text files

	def getAssayIDs(self):
		# handle query 0 : returns { assay key : MGI ID }
		ids = {}

		cols, rows = self.results[0]

		idCol = Gatherer.columnNumber (cols, 'accID')
		keyCol = Gatherer.columnNumber (cols, '_Assay_key')

		for row in rows:
			ids[row[keyCol]] = row[idCol]

		logger.debug ('Got %d assay IDs' % len(ids))
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
		return symbol

	def getDisplayableImagePanes(self):
		# handle query 3 : returns { image pane key : 1 }
		panes = {}

		cols, rows = self.results[3]

		for row in rows:
			panes[row[0]] = 1

		logger.debug ('Got %d displayable image panes' % len(panes))
		return panes

	def getRecombinaseReporterGeneKeys(self):
		# handle query 4 : returns { recomb. reporter gene key : 1 }
		genes = {}

		cols, rows = self.results[4]

		for row in rows:
			genes[row[0]] = 1

		logger.debug('Got %d recombinase reporter genes' % len(genes))
		return genes

	def getPanesForInSituResults(self):
		# handle query 5 : returns { assay key : [ image pane keys ] }
		panes = {}

		cols, rows = self.results[5]

		assayCol = Gatherer.columnNumber (cols, '_Result_key')
		paneCol = Gatherer.columnNumber (cols, '_ImagePane_key')

		for row in rows:
			assay = row[assayCol]
			if panes.has_key(assay):
				panes[assay].append (row[paneCol])
			else:
				panes[assay] = [ row[paneCol] ]

		logger.debug ('Got panes for %d in situ assays' % len(panes))
		return panes

	def getBasicAssayData(self):
		# handle query 6 : returns [ [ assay key, assay type,
		#	reference key, marker key, image pane key,
		#	reporter gene key, is gel flag ], ... ]
		assays = []

		cols, rows = self.results[6]

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
		return assays

	def getInSituExtraData(self):
		# handle query 7 : returns { assay key : [ genotype key,
		#	age, strength, result key, structure key ] }
		extras = {}

		cols, rows = self.results[7]

		assayCol = Gatherer.columnNumber (cols, '_Assay_key')
		genotypeCol = Gatherer.columnNumber (cols, '_Genotype_key')
		ageCol = Gatherer.columnNumber (cols, 'age')
		strengthCol = Gatherer.columnNumber (cols, 'strength')
		resultCol = Gatherer.columnNumber (cols, '_Result_key')
		structureCol = Gatherer.columnNumber (cols,'_Structure_key')
		ageMinCol = Gatherer.columnNumber (cols, 'ageMin')
		ageMaxCol = Gatherer.columnNumber (cols, 'ageMax')
		patternCol = Gatherer.columnNumber (cols, 'pattern')

		for row in rows:
		    assayKey = row[assayCol]

		    if extras.has_key(assayKey):
	 		extras[assayKey].append( [ row[genotypeCol],
				row[ageCol], row[strengthCol], row[resultCol],
				row[structureCol], row[ageMinCol],
				row[ageMaxCol], row[patternCol] ] )
		    else:
	 	 	extras[assayKey] = [ [ row[genotypeCol],
		 		row[ageCol], row[strengthCol], row[resultCol],
		 		row[structureCol], row[ageMinCol],
		 		row[ageMaxCol], row[patternCol] ] ]


		logger.debug ('Got extra data for %d in situ assays' % \
			len(extras))
		return extras

	def getGelExtraData(self):
		# handle query 8 : returns { assay key : [ genotype key,
		#	age, strength, structure key ] }
		extras = {}

		cols, rows = self.results[8]

		assayCol = Gatherer.columnNumber (cols, '_Assay_key')
		genotypeCol = Gatherer.columnNumber (cols, '_Genotype_key')
		ageCol = Gatherer.columnNumber (cols, 'age')
		strengthCol = Gatherer.columnNumber (cols, 'strength')
		structureCol = Gatherer.columnNumber (cols,'_Structure_key')
		ageMinCol = Gatherer.columnNumber (cols, 'ageMin')
		ageMaxCol = Gatherer.columnNumber (cols, 'ageMax')
		gelLaneCol = Gatherer.columnNumber (cols, '_GelLane_key')

		# as a pre-processing step, compute the strength for each
		# gel lane / structure pair

		strengths = {}		# gel lane, structure -> strength

		for row in rows:
			gelLane = row[gelLaneCol]
			structure = row[structureCol]
			strength = row[strengthCol]

			pair = (gelLane, structure)

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
		    structure = row[structureCol]

		    pair = (gelLane, structure)
		    strength = strengths[pair]

		    # if we have already added a row for this pair, then we
		    # can move on to the next one

		    if seen.has_key(pair):
			    continue

		    seen[pair] = 1

		    if extras.has_key(assayKey):
			extras[row[assayCol]].append( [ row[genotypeCol],
				row[ageCol], strength,
				row[structureCol], row[ageMinCol],
				row[ageMaxCol] ] )
		    else:
			extras[row[assayCol]] = [ [ row[genotypeCol],
				row[ageCol], strength,
				row[structureCol], row[ageMinCol],
				row[ageMaxCol] ] ]

		logger.debug ('Got extra data for %d gel assays' % \
			len(extras))
		return extras

	def sortSystems(self):
		cols, rows = self.results[10]

		self.systemSequenceNum = {}
		i = 0

		for row in rows:
			i = i + 1
			self.systemSequenceNum[row[0]] = i
		logger.debug ('Sorted %d systems' % i)
		return

	def sortGenotypeData(self):
		cols, rows = self.results[9]

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
		return

	def getWildTypeAssays (self):
		# get a dictionary with two types of keys
		#	genotype key : where the genotype is always wild-type
		#	(genotype key, assay key) : where the genotype is
		#		sometimes wild-type, depending partly on the
		#		type of assay with which it is associated

		wildtype = {}

		# query 11 - genotypes that are always wild-type

		cols, rows = self.results[11]

		for row in rows:
			wildtype[row[0]] = 1

		logger.debug ('Found %d wild-type genotypes' % len(rows))

		# query 12 - genotype/assay pairs that are wild-type together

		cols, rows = self.results[12]

		genotypeCol = Gatherer.columnNumber (cols, '_Genotype_key')
		assayCol = Gatherer.columnNumber (cols, '_Assay_key')

		for row in rows:
			wildtype[(row[genotypeCol], row[assayCol])] = 1

		logger.debug ('Found %d wild-type genotype/assay pairs' % \
			len(rows))

		return wildtype

	def collateResults (self):

		# get caches of data

		assayIDs = self.getAssayIDs()
		jNumbers = self.getJnumbers()
		symbols = self.getMarkerSymbols()
		displayablePanes = self.getDisplayableImagePanes()
		recombinaseReporters = self.getRecombinaseReporterGeneKeys()
		panesForInSituResults = self.getPanesForInSituResults()

		assays = self.getBasicAssayData()
		inSituExtras = self.getInSituExtraData()
		gelExtras = self.getGelExtraData()

		wildtype = self.getWildTypeAssays()

		# definitions for expression_result_summary table data

		ersCols = [ 'result_key', '_Assay_key', 'assayType',
			'assayID', '_Marker_key', 'symbol', 'system', 'stage',
			'age', 'ageAbbreviation', 'age_min', 'age_max',
			'structure', 'printname', 'structureKey',
			'detectionLevel', 'isExpressed', '_Refs_key',
			'jnumID', 'hasImage', '_Genotype_key', 'is_wild_type',
			'pattern'
			]
		ersRows = []

		# definitions for expression_result_to_imagepane table data

		ertiCols = [ 'result_key', '_ImagePane_key',
			'sequence_num' ]
		ertiRows = []

		ageMinMax = {}		# result key -> (age min, age max)

		# now, we need to walk through our assays to populate the list
		# of data for each table

		newKey = 0		# unique key for each result

		for [ assayKey, assayType, refsKey, markerKey, imagepaneKey,
		    reporterGeneKey, isGel ] in assays:
			
		    #
		    # select the 'extra' assay information
		    #

		    if isGel:
			extras = gelExtras[assayKey]
		    else:
			extras = inSituExtras[assayKey]

		    for items in extras:

			hasImage = 0	# is there a displayable image for
					# ...this result?

			panes = []	# image panes for this result

			if isGel:
			    [ genotypeKey, age, strength, structureKey,
				ageMin, ageMax ] = items

			    pattern = None

			    if imagepaneKey:
				panes = [ imagepaneKey ]

				if displayablePanes.has_key(imagepaneKey):
				    hasImage = 1
			else:
			    [ genotypeKey, age, strength, resultKey,
				structureKey, ageMin, ageMax, pattern ] = items

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

			newKey = newKey + 1

			outRow = [ newKey,
			    assayKey,
			    assayType,
			    assayIDs[assayKey],
			    markerKey,
			    symbols[markerKey],
			    ADVocab.getSystem(structureKey),
			    ADVocab.getStage(structureKey),
			    age,
			    abbreviate(age),
			    ageMin,
			    ageMax,
			    ADVocab.getStructure(structureKey),
			    ADVocab.getPrintname(structureKey),
			    ADVocab.getTermKey(structureKey),
			    strength,
			    getIsExpressed(strength),
			    refsKey,
			    jNumbers[refsKey],
			    hasImage,
			    genotypeKey,
			    isWildType,
			    pattern
			    ]

			ersRows.append (outRow)

			ageMinMax[newKey] = (ageMin, ageMax)

			for paneKey in panes:
			    paneRow = [ newKey, paneKey, len(ertiRows) + 1 ]
			    ertiRows.append (paneRow)

		logger.debug ('Got %d GXD result summary rows' % len(ersRows))
		logger.debug ('Got %d GXD result/pane rows' % len(ertiRows))

		self.output.append ( (ersCols, ersRows) )
		self.output.append ( (ertiCols, ertiRows) )

		(cols, rows) = self.getSequenceNumTable (ersCols, ersRows,
			ageMinMax)

		self.output.append ( (cols, rows) )
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

	def getSystemSequenceNum (self, system):
		if self.systemSequenceNum.has_key(system):
			return self.systemSequenceNum[system]
		return len(self.systemSequenceNum) + 1

	def getGenotypeSequenceNum (self, genotypeKey):
		if self.byGenotype.has_key(genotypeKey):
			return self.byGenotype[genotypeKey]
		return len(self.byGenotype) + 1

	def getSequenceNumTable (self, ersCols, ersRows, ageMinMax):
		# build and return the rows (and columns) for the
		# expression_result_sequence_num table

		self.sortSystems()
		self.sortGenotypeData()

		cols = [ 'result_key', 'by_assay_type', 'by_gene_symbol',
			'by_anatomical_system', 'by_age', 'by_strucure',
			'by_expressed', 'by_mutant_alleles', 'by_reference' ]
		rows = []

		resultKeys = []		# all result_key values

		# We need to generate one row for each incoming row from 
		# 'ersRows'.  Each of these rows will have multiple pre-
		# computed sorts (as in 'cols'), and each of those needs to be
		# pre-computed as a multi-level sort.

		# Each of these ordering lists is used to pre-compute one of
		# the sorts.  Each list will be populated by tuples containing
		# the values to be considered in the sort for that item, and
		# the tuple's last entry will be the result_key.

		byAssayType = []
		bySymbol = []
		bySystem = []
		byAge = []
		byStructure = []
		byExpressed = []
		byMutants = []
		byReference = []

		# fill in sortable data for each of the ordering lists
		# (right now we only do byStructure)

		resultKeyCol = Gatherer.columnNumber (ersCols, 'result_key')
		assayKeyCol = Gatherer.columnNumber (ersCols, '_Assay_key')
		assayTypeCol = Gatherer.columnNumber (ersCols, 'assayType')
		symbolCol = Gatherer.columnNumber (ersCols, 'symbol')
		systemCol = Gatherer.columnNumber (ersCols, 'system')
		stageCol = Gatherer.columnNumber (ersCols, 'stage')
		expressedCol = Gatherer.columnNumber (ersCols, 'isExpressed')
		refsKeyCol = Gatherer.columnNumber (ersCols, '_Refs_key')
		structureKeyCol = Gatherer.columnNumber (ersCols,
			'structureKey')
		genotypeKeyCol = Gatherer.columnNumber (ersCols,
			'_Genotype_key')

		logger.debug ('identified columns for sorts')

		for row in ersRows:
		    resultKey = row[resultKeyCol]

		    # before getting the sequence number for the structure, we
		    # need to convert from a term key for the structure to its
		    # original structure key in mgd
		    structure = ADVocab.getSequenceNum (
			ADVocab.getStructureKey (row[structureKeyCol]) )

		    stage = self.getStageSequenceNum (row[stageCol])
		    ageMin = ageMinMax[resultKey][0]
		    ageMax = ageMinMax[resultKey][1]
		    expressed = self.getExpressedSequenceNum (row[expressedCol])
		    symbol = self.getSymbolSequenceNum (row[symbolCol])
		    assay = VocabSorter.getAssayTypeSequenceNum (
			row[assayTypeCol])
		    system = self.getSystemSequenceNum(row[systemCol])
		    mutants = self.getGenotypeSequenceNum (row[genotypeKeyCol])
		    refs = ReferenceCitations.getSequenceNum (row[refsKeyCol])

		    # add records to the various individual ordering lists

		    byAssayType.append ( (assay, symbol, ageMin, ageMax,
			    structure, stage, expressed, resultKey) )
		    bySymbol.append ( (symbol, assay, ageMin, ageMax,
			    structure, stage, expressed, resultKey) )
		    bySystem.append ( (system, structure, stage, ageMin,
			    ageMax, expressed, symbol, resultKey) )
		    byAge.append ( (ageMin, ageMax, structure, stage, 
			    expressed, symbol, assay, resultKey) )
		    byStructure.append ( (structure, stage, ageMin, ageMax,
			    expressed, symbol, assay, resultKey) )
		    byExpressed.append ( (expressed, symbol, assay, ageMin,
			    ageMax, structure, stage, resultKey) )
		    byMutants.append ( (mutants, symbol, assay, ageMin,
			    ageMax, structure, stage, resultKey) )
		    byReference.append ( (refs, symbol, assay, ageMin, ageMax,
			    structure, stage, resultKey) )

		    resultKeys.append (resultKey)

		logger.debug ('compiled ordering lists')

		# sort the individual ordering lists

		for orderingList in [ byAssayType, bySymbol, bySystem, byAge,
			byStructure, byExpressed, byMutants, byReference]:
				orderingList.sort()

		logger.debug ('sorted ordering lists')

		# produce the individual output row for each result key

		output = {}		# result key -> [ sort 1, ... n ]

		for key in resultKeys:
			output[key] = [ key ]

		for orderingList in [ byAssayType, bySymbol, bySystem, byAge,
			byStructure, byExpressed, byMutants, byReference ]:
				i = 0

				# remember which result keys were not seen for
				# this orderingList so far

				notDone = {}
				for key in resultKeys:
					notDone[key] = 1

				# order those we know about in 'orderingList'

				for row in orderingList:
					i = i + 1
					output[row[-1]].append(i)
					del notDone[row[-1]]

				# order those left over in 'notDone'

				keys = notDone.keys()
				keys.sort()

				for key in keys:
					i = i + 1
					output[row[-1]].append(i)

		logger.debug ('collated ordering lists into dict')

		# extract the individual rows and bundle them into 'rows'

		keys = output.keys()
		keys.sort()

		for key in keys:
			rows.append (output[key])

		logger.debug ('produced %d ordering rows' % len(rows))
		return cols, rows

###--- globals ---###

cmds = [
	# While it would be nice to make use of the GXD_Expression table to
	# assemble our data, it does not provide access to some information we
	# need, like the raw 'strength' of detection values.  So, we are left
	# building the summary data back up from the base tables.

	#
	# TR10273/notes:
	# The GXD_Expression cache could be changed/added to in order
	# to provide the information required by this gatherer.
	#

	# 0. MGI IDs for expression assays
	'''select _Object_key as _Assay_key,
		accID
	from ACC_Accession
	where _MGIType_key = 8
		and _LogicalDB_key = 1
		and private = 0
		and preferred = 1''',

	# 1. J: numbers (IDs) for references with expression data
	'''select a._Object_key as _Refs_key,
		a.accID
	from ACC_Accession a
	where a._MGIType_key = 1
		and a.private = 0
		and a.preferred = 1
		and a._LogicalDB_key = 1
		and a.prefixPart = 'J:'
		and exists (select 1 from GXD_Expression g
			where a._Object_key = g._Refs_key)''',

	# 2. marker symbols studied in expression data
	'''select m._Marker_key, m.symbol
	from MRK_Marker m
	where exists (select 1 from GXD_Expression g
		where m._Marker_key = g._Marker_key)''',

	# 3. list of image panes which have dimensions (indicates whether we
	# have the actual image available for display or not)

	'''select p._ImagePane_key
	from IMG_ImagePane p, IMG_Image i, VOC_Term t
	where p._Image_key = i._Image_key
		and i.xDim is not null
		and i._ImageClass_key = t._Term_key
		and t.term = 'Expression' ''',

	# 4. set of reporter genes which would indicate a recombinase assay
	# (for assay types where it's not automatically a recombinase assay)
	'''select msm._Object_key as _ReporterGene_key
	from MGI_Set ms, MGI_SetMember msm
	where ms._Set_key = msm._Set_key
		and ms.name = 'Recombinases' ''',

	# 5. image panes for in situ results
	'''select distinct i._Result_key, i._ImagePane_key
	from GXD_InSituResultImage i''',

	# 6. basic assay data (image key here is for gel assays)
	'''select a._Assay_key, a._AssayType_key, t.assayType,
		a._Refs_key, a._Marker_key, a._ImagePane_key,
		a._ReporterGene_key, t.isGelAssay
	from GXD_Assay a, GXD_AssayType t
	where a._AssayType_key = t._AssayType_key
	and exists (select 1 from GXD_Expression e where e._Assay_key = a._Assay_key)''',

	# 7. additional data for in situ assays (note that there can be > 1
	# structures per result key)

	'''select s._Assay_key,
		s._Genotype_key,
		s.age,
		r._Strength_key,
		st.strength,
		r._Result_key,
		rs._Structure_key,
		s.ageMin,
		s.ageMax,
		p.pattern
	from GXD_Specimen s,
		GXD_InSituResult r,
		GXD_Strength st,
		GXD_ISResultStructure rs,
		GXD_Pattern p
	where s._Specimen_key = r._Specimen_key
		and r._Strength_key = st._Strength_key
		and r._Pattern_key = p._Pattern_key
		and r._Result_key = rs._Result_key''', 

	# 8. additional data for gel assays (skip control lanes)  (note that
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
		gs._Structure_key,
		g.ageMin,
		g.ageMax,
		g._GelLane_key
	from GXD_GelLane g,
		GXD_GelBand b,
		GXD_Strength st,
		GXD_GelLaneStructure gs
	where g._GelControl_key = 1
		and g._GelLane_key = b._GelLane_key
		and b._Strength_key = st._Strength_key
		and g._GelLane_key = gs._GelLane_key''',

	# 9. allele pairs for genotypes cited in GXD data
	'''select gap._Genotype_key,
		a1.symbol as symbol1,
		a2.symbol as symbol2
	from GXD_AllelePair gap
	inner join ALL_Allele a1 on (gap._Allele_key_1 = a1._Allele_key)
	left outer join ALL_Allele a2 on (gap._Allele_key_2 = a2._Allele_key)
	where exists (select 1 from GXD_Specimen s
			where s._Genotype_key = gap._Genotype_key)
		or exists (select 1 from GXD_GelLane g
			where g._Genotype_key = gap._Genotype_key)
	order by gap.sequenceNum''',

	# 10. ordered list of systems
	'''select distinct t.term
	from GXD_Structure s,
		VOC_Term t
	where s._System_key = t._Term_key
	order by t.term''',

	# 11. genotypes with no allele pairs (treat expression assays for
	# these as wild type)
	'''select g._Genotype_key
	from GXD_Genotype g
	where g._Genotype_key >= 0
		and not exists (select 1 from GXD_AllelePair p
			where g._Genotype_key = p._Genotype_key)''',

	# 12. also treat expression assays for these genotypes as wild type;
	# these have:
	#	a. assay type = In situ reporter (knock in) (key 9)
	#	b. only one allele pair
	#	c. allele pair with one wild type allele and one other
	#	d. both alleles for the assayed gene
	'''select distinct s._Assay_key,
		s._Genotype_key
	from GXD_Specimen s,
		GXD_Assay a,
		GXD_AlleleGenotype gag1,
		GXD_AlleleGenotype gag2,
		ALL_Allele a1,
		ALL_Allele a2
	where s._Assay_key = a._Assay_key
		and a._AssayType_key = 9
		and s._Genotype_key >= 0
		and exists (select 1 from GXD_Expression e where e._Assay_key = a._Assay_key)
		and not exists (select 1 from GXD_AllelePair gap
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
		'_Marker_key', 'symbol', 'system', 'stage', 'age',
		'ageAbbreviation', 'age_min', 'age_max',
		'structure', 'printname', 'structureKey', 'detectionLevel',
		'isExpressed', '_Refs_key', 'jnumID', 'hasImage',
		'_Genotype_key', 'is_wild_type', 'pattern' ],
		'expression_result_summary'),

	('expression_result_to_imagepane',
		[ Gatherer.AUTO, 'result_key', '_ImagePane_key',
		'sequence_num' ],
		'expression_result_to_imagepane'),

	('expression_result_sequence_num',
		[ 'result_key', 'by_assay_type', 'by_gene_symbol',
		'by_anatomical_system', 'by_age', 'by_strucure',
		'by_expressed', 'by_mutant_alleles', 'by_reference' ],
		'expression_result_sequence_num'),
	]

# global instance of a ExpressionResultSummaryGatherer
gatherer = ExpressionResultSummaryGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
