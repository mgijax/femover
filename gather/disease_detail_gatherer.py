#!/usr/local/bin/python
# 
# gathers data for the table supporting the disease detail page in the
# front-end database

import Gatherer
import logger
import symbolsort
import GenotypeClassifier
import ReferenceCitations

###--- Constants ---###

OMIM_GENOTYPE = 1005		# VOC_AnnotType for 'OMIM/Genotype'
OMIM_HUMAN_MARKER = 1006	# VOC_AnnotType for 'OMIM/Human Marker'
MOUSE = 1			# MGI_Organism for 'mouse, laboratory'
HUMAN = 2			# MGI_Organism for 'human'
NOT_QUALIFIER = 1614157		# VOC_Term for 'not'
DRIVER_NOTE = 1034		# MGI_NoteType for 'Driver'
GT_ROSA = 37270			# MRK_Marker for 'Gt(ROSA)26Sor'

MOUSE_AND_HUMAN = 'mouse and human'	# disease group types
MOUSE_ONLY = 'mouse only'
HUMAN_ONLY = 'human only'
NON_GENES = 'non-genes'
NOT_OBSERVED = 'not observed'
ADDITIONAL = 'additional'

GROUP_ORDER = [ MOUSE_AND_HUMAN, MOUSE_ONLY, HUMAN_ONLY, NON_GENES,
	ADDITIONAL, NOT_OBSERVED ]

DISEASE_GROUP_COUNTER = None	# generates primary keys for disease_group
DISEASE_ROW_COUNTER = None	# generates primary keys for disease_row
DISEASE_MODEL_COUNTER = None	# generates primary keys for disease_model

MARKER_DATA_CACHE = {}		# dict; marker key -> (organism, symbol,
				#	marker type, homology class key)

CLASS_KEY_TO_MARKERS = {}	# dict; class key -> [ marker keys ]

TERM_CACHE = {}			# dict; term key -> (term, primary ID)

DISEASE_MODEL_CACHE = {}	# dict; (genotype key, term key, is not) ->
				#	DiseaseModel object

MARKER_TO_DISEASE_MODELS = {}	# dict; marker key -> [ DiseaseModel objects ]

TERM_TO_DISEASE_MODELS = {}	# dict; term key -> [ DiseaseModel objects ]

DRIVER_ALLELES = None		# dict; allele key -> 1 if it has driver note

###--- Functions ---###

def hasDriverNote (alleleKey):
	# returns 1 if the allele has a driver note, 0 if not

	if DRIVER_ALLELES.has_key(alleleKey):
		return 1
	return 0

def compareRefs (a, b):
	return cmp(ReferenceCitations.getSequenceNum(a),
		ReferenceCitations.getSequenceNum(b) )

def compareDiseaseModels (a, b):
	# assumes a and b are both (disease model key, DiseaseModel object)

	return cmp(a[1].getSequenceNum(), b[1].getSequenceNum())

def getAllDiseaseModels():
	return DISEASE_MODEL_CACHE.values()

def getAdditionalModels (termKey, markerKeys):
	# get any models for termKey which did not have a marker in markerKeys

	global TERM_TO_DISEASE_MODELS

	# no disease models?  short circuit.
	if not DISEASE_MODEL_CACHE:
		return []

	# already populated cache and have term key desired; grab it.

	if TERM_TO_DISEASE_MODELS.has_key(termKey):
		diseaseModels = TERM_TO_DISEASE_MODELS[termKey]

	# need to populate cache, then grab models for that term.

	elif not TERM_TO_DISEASE_MODELS:
		TERM_TO_DISEASE_MODELS = {}

		for dm in getAllDiseaseModels():
			dmTermKey = dm.getDiseaseKey()
			if TERM_TO_DISEASE_MODELS.has_key(dmTermKey):
				TERM_TO_DISEASE_MODELS[dmTermKey].append(dm)
			else:
				TERM_TO_DISEASE_MODELS[dmTermKey] = [ dm ]

		logger.debug ('Cached disease models by %d terms' % \
			len(TERM_TO_DISEASE_MODELS))

		if TERM_TO_DISEASE_MODELS.has_key(termKey):
			diseaseModels = TERM_TO_DISEASE_MODELS[termKey]
		else:
			return []

	# already populated cache, but no models for this term
	else:
		return []

	models = []

	for dm in diseaseModels:
		matchedMarker = False
		for markerKey in dm.getMarkerKeys():
			if markerKey in markerKeys:
				matchedMarker = True
				break

		if not matchedMarker:
			models.append(dm)

	return models


def getDiseaseModelsByMarker (markerKey, termKey):
	# assumes we will not be adding to DISEASE_MODEL_CACHE any more;
	# look up disease models by marker/term pair

	global MARKER_TO_DISEASE_MODELS

	key = (markerKey, termKey) 

	if MARKER_TO_DISEASE_MODELS.has_key(key):
		return MARKER_TO_DISEASE_MODELS[key]

	if MARKER_TO_DISEASE_MODELS:
		# we've already generated the cache, so this marker had no
		# models for this disease
		return []

	if not DISEASE_MODEL_CACHE:
		# there are no disease models
		return []

	for dm in DISEASE_MODEL_CACHE.values():
		diseaseKey = dm.getDiseaseKey()

		for marker in dm.getMarkerKeys():
			key2 = (marker, diseaseKey)

			if not MARKER_TO_DISEASE_MODELS.has_key(key2):
				MARKER_TO_DISEASE_MODELS[key2] = []
			MARKER_TO_DISEASE_MODELS[key2].append (dm)

	logger.debug ('Cached disease models for %d marker/term pairs' % \
		len(MARKER_TO_DISEASE_MODELS) )

	if MARKER_TO_DISEASE_MODELS.has_key(key):
		return MARKER_TO_DISEASE_MODELS[key]

	return []

def getDiseaseModel (genotypeKey, termKey, isNot = 0):
	global DISEASE_MODEL_CACHE

	key = (genotypeKey, termKey, isNot)

	# if we already have one for this trio, return the same one
	if DISEASE_MODEL_CACHE.has_key(key):
		return DISEASE_MODEL_CACHE[key]

	# otherwise, create a new one, cache it, and return it

	dm = DiseaseModel (genotypeKey, termKey, isNot)
	DISEASE_MODEL_CACHE[key] = dm 
	return dm

def term (termKey):
	if TERM_CACHE.has_key(termKey):
		return TERM_CACHE[termKey][0]
	return None

def primaryID (termKey):
	if TERM_CACHE.has_key(termKey):
		return TERM_CACHE[termKey][1]
	return None

def markerKeys (classKey):
	global CLASS_KEY_TO_MARKERS

	# if we've not yet built the cache, then build it using data from the
	# other cache

	if not CLASS_KEY_TO_MARKERS:
		for markerKey in MARKER_DATA_CACHE.keys():
			classKey = MARKER_DATA_CACHE[markerKey][3]

			if CLASS_KEY_TO_MARKERS.has_key(classKey):
				CLASS_KEY_TO_MARKERS[classKey].append (
					markerKey)
			else:
				CLASS_KEY_TO_MARKERS[classKey] = [ markerKey ]

		logger.debug ('Cached marker keys for each homology class')

	# return the markers, if this is a valid class key

	if CLASS_KEY_TO_MARKERS.has_key(classKey):
		return CLASS_KEY_TO_MARKERS[classKey]
	return [] 

def nextDiseaseGroupKey():
	global DISEASE_GROUP_COUNTER

	if not DISEASE_GROUP_COUNTER:
		DISEASE_GROUP_COUNTER = Counter()

	return DISEASE_GROUP_COUNTER.getNext()

def nextDiseaseRowKey():
	global DISEASE_ROW_COUNTER

	if not DISEASE_ROW_COUNTER:
		DISEASE_ROW_COUNTER = Counter()

	return DISEASE_ROW_COUNTER.getNext()

def nextDiseaseModelKey():
	global DISEASE_MODEL_COUNTER

	if not DISEASE_MODEL_COUNTER:
		DISEASE_MODEL_COUNTER = Counter()

	return DISEASE_MODEL_COUNTER.getNext()

def markerType (markerKey):
	if MARKER_DATA_CACHE.has_key(markerKey):
		return MARKER_DATA_CACHE[markerKey][2]
	return None

def organism (markerKey):
	if MARKER_DATA_CACHE.has_key(markerKey):
		return MARKER_DATA_CACHE[markerKey][0]
	return None

def symbol (markerKey):
	if MARKER_DATA_CACHE.has_key(markerKey):
		return MARKER_DATA_CACHE[markerKey][1]
	return None

def clusterKey (markerKey):
	if MARKER_DATA_CACHE.has_key(markerKey):
		return MARKER_DATA_CACHE[markerKey][3]
	return None

def mouseMarkerKeys(classKey):
	mouseKeys = []

	for markerKey in markerKeys(classKey):
		(organism, symbol, markerType, classKey) = \
			MARKER_DATA_CACHE[markerKey]

		if organism == 'mouse':
			mouseKeys.append (markerKey)

	return mouseKeys

def humanMarkerKeys(classKey):
	humanKeys = []

	for markerKey in markerKeys(classKey):
		(organism, symbol, markerType, classKey) = \
			MARKER_DATA_CACHE[markerKey]

		if organism == 'human':
			humanKeys.append (markerKey)

	return humanKeys

def compareDiseaseRows (a, b):
	# assumes 'a' and 'b' are DiseaseRow objects

	return symbolsort.nomenCompare (a.getSortValue(), b.getSortValue())

def compareMarkers (a, b):
	# assumes 'a' and 'b' are:  (marker key, is causative?)

	# sort causative markers first, then non-causative
	# sort by symbol within each of those categories

	aCausative = a[1]
	bCausative = b[1]

	if aCausative == 1:
		if bCausative == 0:
			return -1	# 'a' first
	elif bCausative == 1:
		return 1		# 'b' first

	# if we get here, both 'a' and 'b' have the same causative flag, so
	# we need to sort by symbol

	aSymbol = symbol(a[0])
	bSymbol = symbol(b[0])

	return symbolsort.nomenCompare (aSymbol, bSymbol) 

###--- Classes ---###

class Counter:
	def __init__ (self, initialValue = 1):
		self.lastValue = initialValue - 1
		return

	def getNext (self):
		self.lastValue = self.lastValue + 1
		return self.lastValue

class DiseaseRow:
	def __init__ (self):
		self.diseaseRowKey = nextDiseaseRowKey()
		self.classKey = None
		self.mouseGenes = []
		self.humanGenes = []
		self.mouseSorted = False
		self.humanSorted = False
		self.termKey = None
		self.isNot = 0
		return

	def __str__ (self):
		return 'DiseaseRow [ DR key: %d, term key: %s, HG key: %s, mouse genes: %d, human genes: %d ]' % (self.diseaseRowKey, str(self.termKey), str(self.classKey), len(self.mouseGenes), len(self.humanGenes) )

	def setTermKey (self, termKey):
		self.termKey = termKey
		return

	def getTermKey (self):
		return self.termKey

	def setClassKey (self, classKey):
		self.classKey = classKey
		return

	def setIsNot (self, isNot):
		self.isNot = isNot
		return

	def getIsNot (self):
		return self.isNot

	def addMouse (self, markerKey, isCausative = 0):
		# first addition of a given marker key wins (a later addition
		# of the same marker will not change the isCausative flag)

		for (markerKey1, isCausative1) in self.mouseGenes:
			if markerKey1 == markerKey:
				return
		self.mouseGenes.append ( (markerKey, isCausative) )
		self.mouseSorted = False
		return

	def addHuman (self, markerKey, isCausative = 0):
		# first addition of a given marker key wins (a later addition
		# of the same marker will not change the isCausative flag)

		for (markerKey1, isCausative1) in self.humanGenes:
			if markerKey1 == markerKey:
				return
		self.humanGenes.append ( (markerKey, isCausative) )
		self.humanSorted = False
		return

	def getKey (self):
		return self.diseaseRowKey

	def getClassKey (self):
		return self.classKey

	def getGroupType (self):
		# identify what type of group this row should be part of

		# a non-gene should be alone in the mouse category, with no
		# human markers

		if (len(self.mouseGenes) == 1) and (not self.humanGenes):
			if markerType(self.mouseGenes[0][0]) != 'Gene':
				return NON_GENES

		# We can now divide into three more groups, depending on
		# whether the row has causative mouse markers, causative human
		# markers, or both.

		hasHumanCause = False
		hasMouseCause = False

		for (markerKey, isCausative) in self.mouseGenes:
			if isCausative:
				hasMouseCause = True
				break

		for (markerKey, isCausative) in self.humanGenes:
			if isCausative:
				hasHumanCause = True
				break

		if hasMouseCause:
			if hasHumanCause:
				return MOUSE_AND_HUMAN
			return MOUSE_ONLY

		if hasHumanCause:
			return HUMAN_ONLY

		if self.isNot:
			return NOT_OBSERVED

		return ADDITIONAL

	def getMouseGenes (self):
		if not self.mouseSorted:
			self.mouseGenes.sort(compareMarkers)
			self.mouseSorted = True

		return self.mouseGenes

	def getHumanGenes (self):
		if not self.humanSorted:
			self.humanGenes.sort(compareMarkers)
			self.humanSorted = True

		return self.humanGenes

	def getSortValue (self):
		# We'll sort the row by the first mouse symbol, if one
		# exists and the first human symbol if not.

		sortedGenes = self.getMouseGenes()
		if sortedGenes:
			return symbol(sortedGenes[0][0])

		sortedGenes = self.getHumanGenes()
		if sortedGenes:
			return symbol(sortedGenes[0][0])

		return ''

class DiseaseModel:
	def __init__ (self, genotypeKey, diseaseKey, isNot = 0):
		self.diseaseModelKey = nextDiseaseModelKey()
		self.genotypeKey = genotypeKey
		self.diseaseKey = diseaseKey
		self.isNot = isNot
		self.refs = []
		self.markers = []
		self.refsSorted = True
		self.sequenceNum = 0
		return

	def __str__ (self):
		return 'DiseaseModel [ DM key: %d, geno key: %d, term key: %d, isNot: %d, ref count: %d, marker count: %d ]' % (self.diseaseModelKey, self.genotypeKey, self.diseaseKey, self.isNot, len(self.refs), len(self.markers) )

	def addReferenceKey (self, refsKey):
		if refsKey not in self.refs:
			self.refs.append(refsKey)
			self.refsSorted = False
		return

	def addMarkerKey (self, markerKey):
		if markerKey not in self.markers:
			self.markers.append(markerKey)
		return

	def setSequenceNum (self, sequenceNum):
		self.sequenceNum = sequenceNum
		return

	def getSequenceNum (self):
		return self.sequenceNum

	def getKey (self):
		return self.diseaseModelKey

	def getGenotypeKey (self):
		return self.genotypeKey

	def getDiseaseKey (self):
		return self.diseaseKey

	def getIsNot (self):
		return self.isNot

	def getTerm (self):
		return term(self.diseaseKey)

	def getPrimaryID (self):
		return primaryID(self.diseaseKey)

	def getReferenceKeys (self):
		if not self.refsSorted:
			self.refs.sort(compareRefs)
			self.refsSorted = True
		return self.refs

	def getMarkerKeys (self):
		return self.markers

class DiseaseDetailGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the data needed for the disease detail page
	# Has: queries to execute against the source database
	# Does: queries the source database for disease-related data,
	#	collates results, writes tab-delimited text file

	def buildDiseaseTable (self):
		# side effect:  builds global TERM_CACHE which maps
		# from term key to (term, primary ID)

		global TERM_CACHE

		outColumns = [ '_Term_key', 'term', 'accID', 'name' ]
		outRows = []

		cols, rows = self.results[0]
		keyCol = Gatherer.columnNumber (cols, '_Term_key')
		termCol = Gatherer.columnNumber (cols, 'term')
		idCol = Gatherer.columnNumber (cols, 'accID')
		nameCol = Gatherer.columnNumber (cols, 'name')

		TERM_CACHE = {}

		for row in rows:
			outRows.append ( [
				row[keyCol], row[termCol], row[idCol],
				row[nameCol] ] )

			TERM_CACHE[row[keyCol]] = (row[termCol], row[idCol])

		logger.debug ('Count of disease terms: %d' % len(outRows))
		logger.debug ('Cached %d diseases in TERM_CACHE' % \
			len(TERM_CACHE) )

		return outColumns, outRows

	def buildDiseaseSynonymTable (self):
		cols, rows = self.results[1]
		keyCol = Gatherer.columnNumber (cols, '_Term_key')
		synonymCol = Gatherer.columnNumber (cols, 'synonym')

		# first collect the various synonyms for each term

		synonyms = {}		# term key -> [ synonym 1, ... n ]

		for row in rows:
			termKey = row[keyCol]

			if synonyms.has_key (termKey):
				synonyms[termKey].append (row[synonymCol])
			else:
				synonyms[termKey] = [ row[synonymCol] ]

		logger.debug ('Found disease synonyms: %d' % len(rows))

		# second, sort the synonyms for each term

		termKeys = synonyms.keys()
		termKeys.sort()

		for termKey in termKeys:
			synonyms[termKey].sort (symbolsort.nomenCompare)

		logger.debug ('Sorted synonyms for %d terms' % len(termKeys))

		# third, build the output rows

		outColumns = [ '_Term_key', 'synonym', 'sequenceNum' ]
		outRows = []
		seqNum = 0

		for termKey in termKeys:
			for synonym in synonyms[termKey]:
				seqNum = seqNum + 1
				outRows.append ( [ termKey, synonym, seqNum
					] )

		return outColumns, outRows

	def collectTermMarkerAssociations (self, queryNumber, organism,
			checkGenotype = False):

		# collect the term/marker associations from the specified
		# query number

		diseaseToMarkers = {}		# term key -> [ marker keys ]

		cols, rows = self.results[queryNumber]
		markerCol = Gatherer.columnNumber (cols, '_Marker_key')
		termCol = Gatherer.columnNumber (cols, '_Term_key')

		if checkGenotype:
			genotypeCol = Gatherer.columnNumber (cols,
				'_Genotype_key')

		for row in rows:
			termKey = row[termCol]
			markerKey = row[markerCol]

			if checkGenotype:
				# skip complex, not-conditional genotypes, as
				# we can't identify which marker is tied to
				# the disease.
				if GenotypeClassifier.getClass(
					row[genotypeCol]) == 'cx':
					continue

			if diseaseToMarkers.has_key(termKey):
				if markerKey not in diseaseToMarkers[termKey]:
					diseaseToMarkers[termKey].append (
						markerKey)
			else:
				diseaseToMarkers[termKey] = [ markerKey ]

		logger.debug ('Gatherered %s markers for %d diseases' % (
			organism, len(diseaseToMarkers) ) )

		return diseaseToMarkers

	def buildMarkerCache(self):
		global MARKER_DATA_CACHE

		cols, rows = self.results[4]
		markerCol = Gatherer.columnNumber (cols, '_Marker_key')
		symbolCol = Gatherer.columnNumber (cols, 'symbol')
		organismCol = Gatherer.columnNumber (cols, 'commonName')
		markerTypeCol = Gatherer.columnNumber (cols, 'name')
		clusterCol = Gatherer.columnNumber (cols, '_Cluster_key')

		for row in rows:
			markerKey = row[markerCol]

			# trim the 'laboratory' off of 'mouse'
			organism = row[organismCol].replace(', laboratory','')

			MARKER_DATA_CACHE[markerKey] = (organism,
				row[symbolCol], row[markerTypeCol],
				row[clusterCol])

		logger.debug ('Cached data for %d mouse and human markers' % \
			len(MARKER_DATA_CACHE)) 
		return

	def buildDiseaseModelCache(self):
		global DISEASE_MODEL_CACHE

		cols, rows = self.results[5]

		termCol = Gatherer.columnNumber (cols, '_Term_key')
		markerCol = Gatherer.columnNumber (cols, '_Marker_key')
		genotypeCol = Gatherer.columnNumber (cols, '_Genotype_key')
		qualifierCol = Gatherer.columnNumber (cols, '_Qualifier_key')
		refsCol = Gatherer.columnNumber (cols, '_Refs_key')
		seqNumCol = Gatherer.columnNumber (cols, 'sequenceNum')
		alleleCol = Gatherer.columnNumber (cols, '_Allele_key')

		for row in rows:
			isNot = 0
			if row[qualifierCol] == NOT_QUALIFIER:
				isNot = 1

			dm = getDiseaseModel (row[genotypeCol], row[termCol],
				isNot)
			dm.addReferenceKey (row[refsCol]) 

			# We want to make models for Gt(ROSA), so we can have
			# them appear in the 'additional models' section of
			# the 'all models' page for a disease, but we do not
			# want them tied to a marker for the standard 'disease
			# detail' page.

			if row[markerCol] != GT_ROSA:

				# Likewise, we need disease models for alleles
				# with driver notes, but those genotypes
				# should only be for the 'additional models'
				# section of the 'all models' page for a
				# disease.  They should not participate in
				# marker relationships for the 'disease
				# detail' page.

				if not hasDriverNote (row[alleleCol]):
					dm.addMarkerKey (row[markerCol])

			dm.setSequenceNum (row[seqNumCol])

		logger.debug ('Cached data for %d disease models' % \
			len(DISEASE_MODEL_CACHE))
		return

	def buildRows (self,
		termKey,	# term key for this disease
		cmg, 		# causative mouse genes: [ marker keys ]
		chg 		# causative human genes: [ marker keys ]
		):
		# build rows for this particular disease, for the related
		# tables which share generated keys (disease_group_key,
		# disease_row_key, disease_model_key)

		dg = []		# contents of disease_group for this disease
		dr = []		# contents of disease_row for this disease
		drtm = []	# contents of disease_row_to_marker for...
				#	this disease

		diseaseRows = []	# list of DiseaseRow objects
		classToDR = {}		# homology class key -> DiseaseRow

		# add causative mouse genes to disease rows

		for mouseKey in cmg:
			classKey = clusterKey(mouseKey)

			if classKey:
				if classToDR.has_key(classKey):
					diseaseRow = classToDR[classKey]
				else:
					diseaseRow = DiseaseRow()
					diseaseRow.setTermKey(termKey)
					diseaseRow.setClassKey(classKey)
					classToDR[classKey] = diseaseRow
					diseaseRows.append(diseaseRow)
			else:
				diseaseRow = DiseaseRow()
				diseaseRow.setTermKey(termKey)
				diseaseRows.append(diseaseRow)

			diseaseRow.addMouse (mouseKey, 1)

		# add causative human genes to disease rows

		for humanKey in chg:
			classKey = clusterKey(humanKey)

			if classKey:
				if classToDR.has_key(classKey):
					diseaseRow = classToDR[classKey]
				else:
					diseaseRow = DiseaseRow()
					diseaseRow.setClassKey(classKey)
					diseaseRow.setTermKey(termKey)
					classToDR[classKey] = diseaseRow
					diseaseRows.append(diseaseRow)
			else:
				diseaseRow = DiseaseRow()
				diseaseRow.setTermKey(termKey)
				diseaseRows.append(diseaseRow)

			diseaseRow.addHuman (humanKey, 1) 

		# add non-causative mouse and human genes to disease rows
		# (relies upon the DiseaseRow ignoring any markers it has
		# already seen)

		for classKey in classToDR.keys():
			for mouseKey in mouseMarkerKeys(classKey):
				classToDR[classKey].addMouse (mouseKey, 0)

			for humanKey in humanMarkerKeys(classKey):
				classToDR[classKey].addHuman (humanKey, 0)

		# get any additional models (those DiseaseModels which had no
		# markers appear for this disease already)

		mouseMarkers = []
		for dr in diseaseRows:
			for (key, isCausative) in dr.getMouseGenes():
				if key not in mouseMarkers:
					mouseMarkers.append(key)

		additionalModels = getAdditionalModels (termKey, mouseMarkers)
		additionalModelMap = {}		# disease row key -> model

		for model in additionalModels:
			diseaseRow = DiseaseRow()
			diseaseRow.setTermKey(termKey)
			diseaseRows.append(diseaseRow)

			additionalModelMap[diseaseRow.getKey()] = model
			if model.getIsNot():
				diseaseRow.setIsNot(1)

		# filter the disease rows into their respective groups so we
		# can sort them

		groups = {}		# group type -> [ DiseaseRow objects ]

		for diseaseRow in diseaseRows:
			groupType = diseaseRow.getGroupType()

			if groups.has_key(groupType):
				groups[groupType].append (diseaseRow)
			else:
				groups[groupType] = [ diseaseRow ]

		# now we can sort the DiseaseRow objects in each group

		for groupType in groups.keys():
			groups[groupType].sort(compareDiseaseRows)

		# At this point, we have our disease rows for this disease.
		# They are in their respective groups and are sorted.  And,
		# each has their markers sorted.  So, we can start composing
		# the rows we came here to build.

		# disease_group rows, each (disease group key, disease term
		# 	key, group type, and sequence num)
		dg = []

		# disease_row rows, each (disease row key, disease group key,
		#	sequence num, and homology class key)
		dr = []

		# disease_row_to_marker rows, each (disease row key, marker
		#	key, sequence num, is causative flag?, and organism)
		drtm = []

		groupSeqNum = 0		# sequence number for disease groups

		for groupType in GROUP_ORDER:

			# if this disease doesn't have any rows for this
			# group type, skip it
			if not groups.has_key(groupType):
				continue

			groupSeqNum = groupSeqNum + 1
			diseaseGroupKey = nextDiseaseGroupKey()

			# one disease_group row for this disease/group pair
			dg.append ( (diseaseGroupKey, termKey, groupType,
				groupSeqNum) )

			rowSeqNum = 0
			for diseaseRow in groups[groupType]:
				rowSeqNum = rowSeqNum + 1

				drKey = diseaseRow.getKey()

				# one disease_row row for each row that will
				# appear in the displayed table
				dr.append ( (drKey, diseaseGroupKey,
					rowSeqNum, diseaseRow.getClassKey())
					)

				# one row in disease_row_to_marker for each
				# marker symbol to be displayed in the row

				genes = diseaseRow.getMouseGenes() + \
					diseaseRow.getHumanGenes()

				mrkSeqNum = 0

				for (markerKey, isCausative) in genes:
					mrkSeqNum = mrkSeqNum + 1

					drtm.append ( (drKey, markerKey,
						mrkSeqNum, isCausative,
						organism(markerKey)) )

		return dg, dr, drtm, diseaseRows, additionalModelMap

	def processDiseaseModels(self, diseaseRows, allAdditionalModels):
		dr2mod = []	# disease_row_to_model rows
		dmRows = []	# disease_model rows
		dm2ref = []	# disease_model_to_reference rows

		seqNum = 0	# sequence num for disease_row_to_model
		refSeqNum = 0	# sequence num for disease_model_to_reference

		# convert additional models to be dictionary-accessed

		additionalModelMap = {}
		for (drKey, dm) in allAdditionalModels:
			additionalModelMap[drKey] = dm

		# first compile disease_row_to_model rows

		for dr in diseaseRows:
			drKey = dr.getKey()
			termKey = dr.getTermKey()

			diseaseModels = {}	# disease model key -> dm obj.

			# consolidate the list of disease models for all
			# markers in this disease row

			for (markerKey, isCausative) in dr.getMouseGenes():
			    if isCausative:
				for dm in getDiseaseModelsByMarker(markerKey, termKey):
					diseaseModels[dm.getKey()] = dm
			    else:
				# even non-causative markers still need their
				# models to be included

				for dm in getDiseaseModelsByMarker(markerKey, termKey):
#				    if dm.getIsNot() == 1:
				    diseaseModels[dm.getKey()] = dm

			# any additional models to bring in?

			if additionalModelMap.has_key(drKey):
				dm = additionalModelMap[drKey]
				diseaseModels[dm.getKey()] = dm

			# now we need to order the disease models and
			# begin composing rows for:
			#	disease_row_to_model
			#	disease_model
			#	disease_model_to_reference

			# [ (disease model key, DiseaseModel object), ... ]
			dmList = diseaseModels.items()
			dmList.sort(compareDiseaseModels)

			for (dmKey, dm) in dmList:
				seqNum = seqNum + 1
				dr2mod.append ([ drKey, dmKey, seqNum ])

		subtotal = len(dr2mod)
		logger.debug ('Got %d disease row to model rows' % subtotal)

		# next compile rows for disease_model and
		# disease_model_to_reference

		for dm in getAllDiseaseModels():
			dmKey = dm.getKey()
			dmRows.append ([ dmKey, dm.getGenotypeKey(),
				dm.getTerm(), dm.getPrimaryID(), dm.getIsNot()
				])

			# references for a disease model are already sorted
			# within that object

			for refKey in dm.getReferenceKeys():
				refSeqNum = refSeqNum + 1
				dm2ref.append ([ dmKey, refKey, refSeqNum ])

		logger.debug ('Got %d disease models' % len(dmRows))
		logger.debug ('Got %d disease model to ref rows' % len(dm2ref)) 

		return dr2mod, dmRows, dm2ref

	def buildDriverNoteCache (self):
		# build the cache of alleles with driver notes

		global DRIVER_ALLELES

		DRIVER_ALLELES = {}

		cols, rows = self.results[8]
		alleleCol = Gatherer.columnNumber (cols, '_Allele_key')

		for row in rows:
			DRIVER_ALLELES[row[alleleCol]] = 1
		
		logger.debug ('Cached %d alleles with driver notes' % \
			len(DRIVER_ALLELES))
		return 

	def collateResults (self):
		# main method for pulling the various results sets together

		# step 0 - disease table
		columns, rows = self.buildDiseaseTable()
		self.output.append ( (columns, rows) )

		# step 1 - disease_synonym table
		columns, rows = self.buildDiseaseSynonymTable()
		self.output.append ( (columns, rows) )

		# write tables produced so far, to free up their memory
		self.writeOneFile()
		self.writeOneFile()

		# step 1a - build driver note cache
		self.buildDriverNoteCache()

		# step 2 - gather term/human marker associations
		termToHumanMarkers = self.collectTermMarkerAssociations(2,
			'human')

		# step 3 - gather term/mouse marker associations
		termToMouseMarkers = self.collectTermMarkerAssociations(3,
			'mouse', checkGenotype = True)

		# step 4 - get a list of distinct disease term keys

		termKeys = []
		cols, rows = self.results[7]

		for row in rows:
			termKeys.append (row[0])
		termKeys.sort()

		logger.debug ('Found %d distinct diseases' % len(termKeys))

		# step 5 - cache basic data for mouse and human markers

		self.buildMarkerCache()

		# step 6 - build DiseaseModel objects and cache them

		self.buildDiseaseModelCache()

		#---------- now the interesting part ----------#

		# We need to produce several groups of data rows for each
		# disease, including for when the disease is associated to:
		# 1. both mouse and human genes
		# 2. mouse genes, but not human genes
		# 3. human genes, but not mouse genes
		# 4. mouse marker types other than gene (transgenes, etc.)
		# 5. genotypes expected to model the disease that did NOT

		# Each of those data rows can be associated with:
		# a. a HomoloGene homology class/cluster (only items 1-3)
		# b. zero or more genotypes for mouse models (not item 3)
		# c. zero or more mouse markers
		# d. zero or more human markers

		# Each associated marker (from c and d) will have a
		# pre-computed sequence number and a flag for whether that
		# marker causes the disease.

		# step 7 - build rows for each related table for each disease,
		# concatenating rows into a list for each table

		# lists of rows for all diseases

		allGroups = []		# rows for disease_group table
		allRows = []		# rows for disease_row table
		allRowsToMarkers = []	# rows for disease_row_to_marker table

		allDiseaseRowObjects = []	# list of DiseaseRow objects

		allAdditionalModels = []	# (DiseaseRow key, DiseaseModel)

		for termKey in termKeys:
			cmg = []	# causative mouse genes
			chg = []	# causative human genes

			if termToMouseMarkers.has_key(termKey):
				cmg = termToMouseMarkers[termKey]
			if termToHumanMarkers.has_key(termKey):
				chg = termToHumanMarkers[termKey]

			diseaseGroups, diseaseRows, diseaseRowsToMarkers, \
				diseaseRowObjects, additionalModelMap = \
					self.buildRows (termKey, cmg, chg)

			allGroups = allGroups + diseaseGroups
			allRows = allRows + diseaseRows
			allRowsToMarkers = allRowsToMarkers + \
				diseaseRowsToMarkers
			allDiseaseRowObjects = allDiseaseRowObjects + \
				diseaseRowObjects
			allAdditionalModels = allAdditionalModels + \
				additionalModelMap.items()

		logger.debug ('Compiled %d disease groups' % len(allGroups))
		logger.debug ('Compiled %d disease rows' % len(allRows))
		logger.debug ('Compiled %d disease row/marker pairs' % \
			len(allRowsToMarkers))
		logger.debug ('Collected %d DiseaseRow objects' % \
			len(allDiseaseRowObjects))
		logger.debug ('Found %d additional model rows' % \
			len(allAdditionalModels))

		# disease_group columns
		dgc = [ 'diseaseGroupKey', 'diseaseKey', 'groupType',
			'sequenceNum' ]

		# disease_row columns
		drc = [ 'diseaseRowKey', 'diseaseGroupKey', 'sequenceNum',
			'_Cluster_key' ]

		# disease_row_to_marker columns
		drtm = [ 'diseaseRowKey', '_Marker_key', 'sequenceNum',
			'isCausative', 'organism' ]

		self.output.append ( (dgc, allGroups) )
		self.output.append ( (drc, allRows) )
		self.output.append ( (drtm, allRowsToMarkers) )

		self.writeOneFile()	# write out contents to save memory
		self.writeOneFile()
		self.writeOneFile()

		# step 8 - now we need to relate the DiseaseRow objects to the
		# DiseaseModel objects to be able to produce three more
		# tables:  disease_row_to_model, disease_model, and
		# disease_model_to_reference

		dr2mod, dm, dm2ref = \
			self.processDiseaseModels(allDiseaseRowObjects,
				allAdditionalModels)

		# disease_model columns
		dmc = [ 'diseaseModelKey', 'genotypeKey', 'disease',
			'primaryID', 'isNotModel' ]

		# disease_row_to_model columns
		dr2modc = [ 'diseaseRowKey', 'diseaseModelKey',
			'sequenceNum' ]

		# disease_model_to_reference columns
		dm2refc = [ 'diseaseModelKey', 'refsKey', 'sequenceNum' ]

		self.output.append ( (dmc, dm) )
		self.output.append ( (dr2modc, dr2mod) )
		self.output.append ( (dm2refc, dm2ref) )
		return

###--- globals ---###

cmds = [
	# 0. basic data for disease table
	'''select t._Term_key, t.term, a.accID, d.name
	from voc_term t, acc_accession a, acc_logicaldb d
	where t._Term_key = a._Object_key
		and t._Vocab_key = 44
		and a.preferred = 1
		and a.private = 0
		and a._MGIType_key = 13
		and a._LogicalDB_key = d._LogicalDB_key''',

	# 1. basic data for the disease_synonym table (synonyms need to be
	# ordered in code)
	'''select t._Term_key, s.synonym
	from mgi_synonym s, voc_term t
	where s._MGIType_key = 13
		and s._Object_key = t._Term_key
		and t._Vocab_key = 44
	order by t._Term_key''',

	# 2. all disease annotations to human markers
	'''select vt._Term_key, mm._Marker_key, mo.commonName
	from voc_annot va, voc_term vt, mrk_marker mm, mgi_organism mo
	where va._AnnotType_key = %d
		and va._Term_key = vt._Term_key
		and va._Object_key = mm._Marker_key
		and va._Qualifier_key != %d
		and mm._Organism_key = mo._Organism_key''' % \
			(OMIM_HUMAN_MARKER, NOT_QUALIFIER),

	# 3. pull OMIM annotations up from genotypes to mouse markers,
	# excluding annotations with NOT qualifiers,  alleles with driver
	# notes, and wild-type alleles
	'''select distinct va._Term_key, gag._Marker_key, mo.commonName,
		gg._Genotype_key
	from gxd_genotype gg,
		gxd_allelegenotype gag,
		voc_annot va,
		all_allele a,
		mrk_marker m,
		mgi_organism mo
	where gg._Genotype_key = gag._Genotype_key
		and gg._Genotype_key = va._Object_key
		and va._AnnotType_key = %d
		and va._Qualifier_key != %d
		and gag._Allele_key = a._Allele_key
		and a.isWildType = 0
		and a._Marker_key = m._Marker_key
		and m._Organism_key = mo._Organism_key
		and gag._Marker_key != %d
		and not exists (select 1 from MGI_Note mn
			where mn._NoteType_key = %d
			and mn._Object_key = gag._Allele_key)
	order by gag._Marker_key''' % \
		(OMIM_GENOTYPE, NOT_QUALIFIER, GT_ROSA, DRIVER_NOTE),

	# 4. all current and interim mouse and human markers' basic data,
	
	'''select distinct m._Marker_key,
		m.symbol,
		o.commonName, 
		t.name, 
		mcm._Cluster_key
	from mrk_marker m
	inner join mgi_organism o on (m._Organism_key = o._Organism_key)
	inner join mrk_types t on (m._Marker_Type_key = t._Marker_Type_key)
	left outer join mrk_clustermember mcm on (
		m._Marker_key = mcm._Marker_key)
	left outer join mrk_cluster mc on (mcm._Cluster_key = mc._Cluster_key)
	left outer join voc_term vt on (mc._ClusterSource_key = vt._Term_key
		and vt.term = 'HomoloGene')
	where m._Marker_Status_key in (1,3)
	and m._Organism_key in (1,2)''', 

	# do we bring in all the human and mouse markers for the homology
	# cluster (query 4) and only flag those with actual annotations
	# (query 2 and query 3)?  Yes.

	# 5. finally, we need to pull enough information up to allow creation
	# of disease model objects with their references and their
	# relationships to disease rows.  We INCLUDE annotations with NOT
	# qualifiers this time, but exclude annotations for alleles with
	# driver notes, and wild-type alleles
	'''select distinct va._Term_key, gag._Marker_key, gg._Genotype_key,
		va._Qualifier_key, ve._Refs_key, gag.sequenceNum,
		gag._Allele_key
	from gxd_genotype gg,
		gxd_allelegenotype gag,
		voc_annot va,
		all_allele a,
		mrk_marker m,
		mgi_organism mo,
		voc_evidence ve
	where gg._Genotype_key = gag._Genotype_key
		and gg._Genotype_key = va._Object_key
		and va._AnnotType_key = %d
		and va._Annot_key = ve._Annot_key
		and gag._Allele_key = a._Allele_key
		and a.isWildType = 0
		and a._Marker_key = m._Marker_key
		and m._Organism_key = mo._Organism_key
	order by gag._Marker_key''' % \
		OMIM_GENOTYPE,

	# 6. pull out all disease models (including complex, not conditionals)
	# so we can find those that we don't already have tied to a disease
	# row
	'''select distinct va._Term_key, gag._Marker_key, gg._Genotype_key,
		va._Qualifier_key, ve._Refs_key, gag.sequenceNum
	from gxd_genotype gg,
		gxd_allelegenotype gag,
		voc_annot va,
		all_allele a,
		mrk_marker m,
		mgi_organism mo,
		voc_evidence ve
	where gg._Genotype_key = gag._Genotype_key
		and gg._Genotype_key = va._Object_key
		and va._AnnotType_key = %d
		and va._Annot_key = ve._Annot_key
		and gag._Allele_key = a._Allele_key
		and a.isWildType = 0
		and a._Marker_key = m._Marker_key
		and m._Organism_key = mo._Organism_key
	order by gag._Marker_key''' % \
		OMIM_GENOTYPE,

	# 7. pull out all OMIM term keys used in annotations
	'''select distinct _Term_key
	from voc_annot
	where _AnnotType_key in (%d, %d)''' % (OMIM_GENOTYPE,
		OMIM_HUMAN_MARKER),

	# 8. pull out allele keys that should be excluded from marker/disease
	# associations because of a driver note on the allele (where those
	# genotypes have a disease annotation.
	'''select distinct gag._Allele_key
	from gxd_allelegenotype gag,
		mgi_note mn
	where gag._Allele_key = mn._Object_key
		and mn._NoteType_key = %d
		and exists (select 1 from voc_annot va
			where gag._Genotype_key = va._Object_key
				and va._AnnotType_key = %d)''' % (
					DRIVER_NOTE, OMIM_GENOTYPE),
	]

# Both the 'disease' and 'disease_synonym' tables could be split off into
# separate gatherers, but many of the other related tables are tied by
# generated keys, so we'll just group them all here for simplicity.
files = [
	('disease',
		[ '_Term_key', 'term', 'accID', 'name' ],
		'disease'),

	('disease_synonym',
		[ Gatherer.AUTO, '_Term_key', 'synonym', 'sequenceNum' ],
		'disease_synonym'),

	('disease_group',
		[ 'diseaseGroupKey', 'diseaseKey', 'groupType', 'sequenceNum' ],
		'disease_group'),

	('disease_row',
		[ 'diseaseRowKey', 'diseaseGroupKey', 'sequenceNum',
			'_Cluster_key' ],
		'disease_row'),

	('disease_row_to_marker',
		[ Gatherer.AUTO, 'diseaseRowKey', '_Marker_key', 'sequenceNum',
			'isCausative', 'organism' ],
		'disease_row_to_marker'),

	('disease_model',
		[ 'diseaseModelKey', 'genotypeKey', 'disease', 'primaryID',
			'isNotModel' ],
		'disease_model'),

	('disease_row_to_model',
		[ Gatherer.AUTO, 'diseaseRowKey', 'diseaseModelKey',
			'sequenceNum' ],
		'disease_row_to_model'),

	('disease_model_to_reference',
		[ Gatherer.AUTO, 'diseaseModelKey', 'refsKey', 'sequenceNum' ],
		'disease_model_to_reference'),

	]

# global instance of a DiseaseDetailGatherer
gatherer = DiseaseDetailGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)