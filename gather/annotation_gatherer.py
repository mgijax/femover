#!/usr/local/bin/python
# 
# gathers data for the 'annotation_*' and '*_to_annotation' tables in the
# front-end database

import dbAgnostic
import Gatherer
import VocabSorter
import logger
import GOFilter
import GenotypeClassifier
import symbolsort
import AlleleAndGenotypeSorter
import AnnotationKeyGenerator
import gc
import utils

###--- Constants ---###

MARKER = 2		# MGI Type for markers
GENOTYPE = 12		# MGI Type for genotypes
OMIM_GENOTYPE = 1005	# VOC_AnnotType : OMIM/Genotype
OMIM_MARKER = 1016	# VOC_AnnotType : OMIM/Marker (Derived)
MP_MARKER = 1015	# VOC_AnnotType : MP/Marker (Derived)
MP_GENOTYPE = 1002	# VOC_AnnotType : MP/Genotype
GT_ROSA = 37270		# marker Gt(ROSA)26Sor
DRIVER_NOTE = 1034	# MGI_NoteType Driver
NOT_QUALIFIER = 1614157	# VOC_Term NOT

BUDDING_YEAST_LDB = 114		# logical database key for budding yeast
BUDDING_YEAST = 'budding yeast'	# organism name for budding yeast
FISSION_YEAST_LDB = 115		# logical database key for fission yeast
FISSION_YEAST = 'fission yeast'	# organism name for fission yeast
CHEBI_LDB = 127			# logical database key for ChEBI

EVIDENCE_ID_TABLE = 'evid'

###--- Globals ---###

MARKER_DATA = {}	# marker key : [ symbol, name, ID, logical db, chrom,
			# 	sequence number]

# used to generate new annotation keys for the front-end database for the
# majority of data cases.  (Special cases will use their own key generators
# near where they generate keys.  These will have different rules.)
KEY_GENERATOR = AnnotationKeyGenerator.EvidenceInferredKeyGenerator()

# used to generate new annotation keys for the front-end database for the
# Mammalian Phenotype/Genotype annotations, which should not consider
# evidence codes or inferred-from IDs in identifying distinct annotations.
MPG_KEY_GENERATOR = AnnotationKeyGenerator.KeyGenerator()

###--- Functions ---###

def initialize():
	# build any temp tables needed to support the standard queries

	logger.debug('Entered initialize()...')

	# temp table of seq IDs cited as evidence (inferred-from) for GO
	# annotations

	cmd1 = '''select distinct accID
		into temporary table %s
		from acc_accession
		where _MGIType_key = 25
		and _LogicalDB_key in (68, 9, 13)''' % EVIDENCE_ID_TABLE

	cmd2 = 'create unique index %s_idx1 on %s (accID)' % (
		EVIDENCE_ID_TABLE, EVIDENCE_ID_TABLE)

	dbAgnostic.execute(cmd1)
	dbAgnostic.execute(cmd2)

	logger.debug('  - built %s' % EVIDENCE_ID_TABLE)
	return

# annotation keys to be omitted (for GO ND annotations)
# 	annotKeyToSkip[annot key] = 1
annotKeyToSkip = {}

def getAnnotAttributes (annotKey):
	# returns (annot type, object key, term key, qualifier) if available,
	#	or (None, None, None, None)

	return AnnotationKeyGenerator.getAnnotationAttributes(annotKey)

def getNewAnnotationKey (annotKey, evidenceTermKey, inferredFrom):
	# used to get a new annotation key for traditional (not specially
	# constructed, aka hacked in) annotations.  Masks the complexity of
	# MP/Genotype annotations versus other annotations.

	global KEY_GENERATOR, MPG_KEY_GENERATOR

	aType = AnnotationKeyGenerator.getAnnotationAttributes(annotKey)[0]

	if aType == MP_GENOTYPE:
		return MPG_KEY_GENERATOR.getKey(annotKey)

	key =  KEY_GENERATOR.getKey (annotKey,
		evidenceTermKey = evidenceTermKey,
		inferredFrom = inferredFrom)

	return key

def getMarker (markerKey):
	if MARKER_DATA.has_key(markerKey):
		return MARKER_DATA[markerKey]
	return None, None, None, None, None, None
	
def getSymbol (markerKey):
	return getMarker(markerKey)[0]

def getName (markerKey):
	return getMarker(markerKey)[1]

def getChromosome (markerKey):
	return getMarker(markerKey)[4]

def getID (markerKey):
	return getMarker(markerKey)[2]

def getLogicalDB (markerKey):
	return getMarker(markerKey)[3]

def getMarkerSeqNum (markerKey):
	return getMarker(markerKey)[5]

def compareMarkers (a, b):
	# sort marker tuples (symbol, name, key) as smart-alpha on symbol then
	# name, and fall back on marker key if symbol + name match

	aSym, aName, aKey = a
	bSym, bName, bKey = b

	sCmp = symbolsort.nomenCompare(aSym, bSym)
	if sCmp != 0:
		return sCmp
	
	nCmp = symbolsort.nomenCompare(aName, bName)
	if nCmp != 0:
		return nCmp

	return cmp(aKey, bKey) 

###--- Classes ---###

class OrganismFinder:
	# Is: a mapping from an inferred-from ID to its organism
	# Has: see Is
	# Does: maps from each inferred-from ID to its organism, when we can
	#	track it down

	def __init__ (self):
		self.idCache = {}	# maps from ID -> organism key
		self.organismCache = {}	# maps from organism key -> name

		# has this object been initialized?
		self.initialized = False

		# base query for object lookups; fill in organism key field,
		# extra table(s), and extra where clause(s)
		self.baseQuery = '''select distinct aa.accID, %s
			from voc_evidence ve,
				acc_accession aa,
				voc_annot va,
				acc_accession bb,
				%s
			where ve._AnnotEvidence_key = aa._Object_key
				and aa._MGIType_key = 25
				and ve._Annot_key = va._Annot_key
				and va._AnnotType_key not in (%s, %s)
				and aa.accID = bb.accID
				and aa._LogicalDB_key != %d
				and %s
		''' % ('%s', '%s', OMIM_MARKER, MP_MARKER, CHEBI_LDB, '%s')

		return

	def cacheGeneric(self, cmd, objectType):
		(cols, rows) = dbAgnostic.execute(cmd)

		idCol = dbAgnostic.columnNumber (cols, 'accID')
		organismCol = dbAgnostic.columnNumber (cols, '_Organism_key')

		for row in rows:
			self.idCache[row[idCol]] = row[organismCol]
		logger.debug('Cached organisms for %d %s IDs' % (
			len(rows), objectType))
		return

	def cacheMarkers(self):
		cmd = self.baseQuery % ('m._Organism_key',
			'mrk_marker m',
			'bb._MGIType_key = 2 ' + \
				'and bb._Object_key = m._Marker_key')
		self.cacheGeneric(cmd, 'marker')
		return

	def cacheAlleles(self):
		cmd = self.baseQuery % ('m._Organism_key',
			'all_allele a, mrk_marker m',
			'bb._MGIType_key = 11 ' + \
				'and bb._Object_key = a._Allele_key ' + \
				'and a._Marker_key = m._Marker_key')
		self.cacheGeneric(cmd, 'allele')
		return

	def cacheSequences(self):
		cmd = self.baseQuery % ('s._Organism_key',
			'seq_sequence s',
			'bb._MGIType_key = 19 ' + \
				'and bb._Object_key = s._Sequence_key')
		self.cacheGeneric(cmd, 'sequence')
		return

	def cacheYeast(self):
		cmd = '''select distinct accID from acc_accession
			where _MGIType_key = 25 and _LogicalDB_key = %d'''

		for (name, ldb) in [ (BUDDING_YEAST, BUDDING_YEAST_LDB),
					(FISSION_YEAST, FISSION_YEAST_LDB) ]:
			(cols, rows) = dbAgnostic.execute(cmd % ldb)

			idCol = dbAgnostic.columnNumber (cols, 'accID')

			for row in rows:
				self.idCache[row[idCol]] = name

			self.organismCache[name] = name

			logger.debug('Cached organisms for %d %s IDs' % (
				len(rows), name) )
		return


	def cacheOrganisms(self):
		cmd = 'select _Organism_key, commonName from MGI_Organism'
		(cols, rows) = dbAgnostic.execute(cmd)

		keyCol = dbAgnostic.columnNumber (cols, '_Organism_key')
		nameCol = dbAgnostic.columnNumber (cols, 'commonName')

		for row in rows:
			self.organismCache[row[keyCol]] = \
				utils.cleanupOrganism(row[nameCol])
		logger.debug('Cached %d organisms' % len(self.organismCache))
		return

	def getOrganism(self, accID):
		if not self.initialized:
			self.cacheYeast()
			self.cacheSequences()
			self.cacheAlleles()
			self.cacheMarkers()
			self.cacheOrganisms()
			self.initialized = True

		if self.idCache.has_key(accID):
			return self.organismCache[self.idCache[accID]]
		return None

class AnnotationGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for most of the marker_go_* tables
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for markers' GO
	#	annotations, collates results, writes tab-delimited text files

	def buildAnnotationConsolidator (self):
		cols, rows = self.results[14]

		annotKeyCol = Gatherer.columnNumber (cols, '_Annot_key')
		annotTypeCol = Gatherer.columnNumber (cols, '_AnnotType_key')
		termCol = Gatherer.columnNumber (cols, '_Term_key')
		objectCol = Gatherer.columnNumber (cols, '_Object_key')
		qualifierCol = Gatherer.columnNumber (cols, '_Qualifier_key')

		# maps each annotation key to a single key for shared (type,
		# term, object, qualifier) tuples
		self.uniqueAnnotKey = {}

		# maps (type, term, object, qualifier) to its annotation key
		annotForData = {}

		for row in rows:
			t = (row[annotTypeCol], row[termCol], row[objectCol],
				row[qualifierCol])
			
			# if we've already seen an annotation with these
			# attributes, then we can re-use its annotation key

			if annotForData.has_key(t):
				annotKey = annotForData[t]
				self.uniqueAnnotKey[row[annotKeyCol]] = \
					annotKey
			else:
				annotKey = row[annotKeyCol]
				self.uniqueAnnotKey[annotKey] = annotKey
				annotForData[t] = annotKey

		logger.debug ('Consolidated %d annotation keys down to %d' % \
			(len(rows), len(annotForData)) )

		self.results[14] = (cols, [])
		del rows
		gc.collect()
		logger.debug ('Freed memory for result set 14')
		return

	def translateAnnotKey (self, annotKey):
		return self.uniqueAnnotKey[annotKey]

	def cacheHeaderTerms (self):
		# cache the (currently only) GO header terms for each term

		self.headers = {}

		cols, rows = self.results[15]

		termCol = Gatherer.columnNumber (cols, 'term_key')
		headerCol = Gatherer.columnNumber (cols, 'header_term_key')

		for row in rows:
			termKey = row[termCol]
			headerKey = row[headerCol]

			if self.headers.has_key(termKey):
				self.headers[termKey].append(headerKey)
			else:
				self.headers[termKey] = [ headerKey ]

		logger.debug('Cached %d headers for %d terms' % (
			len(rows), len(self.headers)) )

		self.results[15] = (cols, [])
		gc.collect()
		return

	def buildMarkerCache (self):
		global MARKER_DATA

		cols, rows = self.results[13]

		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		symbolCol = Gatherer.columnNumber (cols, 'symbol')
		nameCol = Gatherer.columnNumber (cols, 'name')
		ldbCol = Gatherer.columnNumber (cols, 'logical_db')
		idCol = Gatherer.columnNumber (cols, 'accID')
		chromCol = Gatherer.columnNumber (cols, 'chromosome')

		for row in rows:
			markerKey = row[markerKeyCol]
			if MARKER_DATA.has_key(markerKey):
				continue

			MARKER_DATA[markerKey] = [
				row[symbolCol], row[nameCol], row[idCol],
				row[ldbCol], row[chromCol],
				]

		logger.debug ('Cached basic data for %d markers' % \
			len(MARKER_DATA))

		self.results[13] = (cols, [])
		del rows
		gc.collect()
		logger.debug ('Freed memory for result set 13')

		toSort = []	# [ (symbol, name, marker key), ... ]
		for (key, (symbol, name, accID, ldb, chrom)) \
			in MARKER_DATA.items():

			toSort.append ( (symbol.lower(), name.lower(), key) )

		toSort.sort (compareMarkers)

		i = 0
		for (symbol, name, key) in toSort:
			i = i + 1
			MARKER_DATA[key].append (i)

		logger.debug ('Sorted %d markers' % len(toSort)) 

		# remember what the highest marker sort value is, so we can
		# put other object types after it for sorts by marker

		self.maxMarkerSequenceNum = i

		del toSort
		gc.collect()
		logger.debug('Freed memory used to sort markers')
		return

	def getTermIDs (self):
		cols, rows = self.results[0]
		keyCol = Gatherer.columnNumber (cols, '_Object_key')
		idCol = Gatherer.columnNumber (cols, 'accID')

		termKeyToID = {}
		for row in rows:
			termKeyToID[row[keyCol]] = row[idCol]

		logger.debug ('Found %d term IDs' % len(termKeyToID))

		self.results[0] = (cols, [])
		del rows
		gc.collect()
		logger.debug('Freed memory for result set 0')

		return termKeyToID

	def getDags (self):
		cols, rows = self.results[1]
		keyCol = Gatherer.columnNumber (cols, '_Term_key')
		dagCol = Gatherer.columnNumber (cols, '_DAG_key')

		termKeyToDagKey = {}
		for row in rows:
			termKeyToDagKey[row[keyCol]] = row[dagCol]

		logger.debug ('Found %d DAG keys for terms' % \
			len(termKeyToDagKey))

		self.results[1] = (cols, [])
		del rows
		gc.collect()
		logger.debug('Freed memory for result set 1')

		return termKeyToDagKey

	def getEvidence (self):
		cols, rows = self.results[2]

		annotKeyCol = Gatherer.columnNumber (cols, '_Annot_key')
		evidenceTermCol = Gatherer.columnNumber (cols,
			'_EvidenceTerm_key')
		jnumCol = Gatherer.columnNumber (cols, 'jnumID')
		refsCol = Gatherer.columnNumber (cols, '_Refs_key')
		inferredFromCol = Gatherer.columnNumber (cols, 'inferredFrom')

		# mgd's _Annot_key -> [ new annotation keys ]
		mgdToNewKeys = {}

		# new annotation key -> evidence key
		annotationEvidence = {}

		# new annotation key -> [ (reference key, jnum ID), ... ]
		annotationRefs = {}

		for row in rows:
			annotKey = self.translateAnnotKey(row[annotKeyCol])

			# if we need to skip this, just move on
			if annotKeyToSkip.has_key(annotKey):
				continue

			evidenceTermKey = row[evidenceTermCol]
			inferredFrom = row[inferredFromCol]

#			newKey = getNewAnnotationKey (annotKey,
#				evidenceTermKey, inferredFrom)

			newKey = getNewAnnotationKey (annotKey,
				evidenceTermKey = evidenceTermKey,
				inferredFrom = inferredFrom)

			if not mgdToNewKeys.has_key(annotKey):
				mgdToNewKeys[annotKey] = [ newKey ]
			elif not newKey in mgdToNewKeys[annotKey]:
				mgdToNewKeys[annotKey].append (newKey)

			if not annotationEvidence.has_key(newKey):
				annotationEvidence[newKey] = \
					row[evidenceTermCol]

			ref = (row[refsCol], row[jnumCol])
			if not annotationRefs.has_key(newKey):
				annotationRefs[newKey] = [ ref ]
			elif not ref in annotationRefs[newKey]:
				annotationRefs[newKey].append(ref)

		logger.debug ('Found evidence info for %d annotations' % \
			len(annotationEvidence) )

		self.results[2] = (cols, [])
		del rows
		gc.collect()
		logger.debug('Freed memory for result set 2')

		return mgdToNewKeys, annotationEvidence, annotationRefs

	def getSpecialIDs (self):
		# get the set of "inferred from" IDs which are sequence IDs
		# from certain providers that we want to redirect to our own
		# sequence detail page

		cols, rows = self.results[4]

		idCol = Gatherer.columnNumber (cols, 'accID')
		ldbKeyCol = Gatherer.columnNumber (cols, '_LogicalDB_key')

		specialIDs = {}
		for row in rows:
			specialIDs[row[idCol]] = row[ldbKeyCol]

		logger.debug ('Found %d seq IDs to send to MGI' % \
			len(specialIDs))
		return specialIDs

	def getInferredFromIDs (self):
		# get the various inferred-from IDs for each annotation

		seqIDs = self.getSpecialIDs()

		cols, rows = self.results[3]

		akeyCol = Gatherer.columnNumber (cols, '_Annot_key')
		evidenceCol = Gatherer.columnNumber(cols, '_EvidenceTerm_key')
		idCol = Gatherer.columnNumber (cols, 'accID')
		ldbKeyCol = Gatherer.columnNumber (cols, '_LogicalDB_key')
		ldbCol = Gatherer.columnNumber (cols, 'logical_db')
		inferredCol = Gatherer.columnNumber (cols, 'inferredFrom')
		preferredCol = Gatherer.columnNumber (cols, 'preferred')

		ids = {}	# new annotation key -> list of (acc ID,
				# ... logical db, preferred)

		for row in rows:
			annotKey = self.translateAnnotKey(row[akeyCol])

			# if we need to skip this, just move on
			if annotKeyToSkip.has_key(annotKey):
				continue

			evidence = row[evidenceCol]
			id = row[idCol]
			ldbKey = row[ldbKeyCol]
			ldbName = row[ldbCol]

			# get the special key we are generating for each
			# unique (annotation key, evidence term key,
			# inferred from) tuple

			newAnnotKey = getNewAnnotationKey (annotKey,
				evidenceTermKey = evidence,
				inferredFrom = row[inferredCol])

			# for certain sequences, we want to have any links go
			# to MGI rather than to the logical database

			if seqIDs.has_key(id):
				if seqIDs[id] == ldbKey:
					ldbKey = 1
					ldbName = 'MGI'

			tpl = (id, ldbName, row[preferredCol])

			# store the ID info for this GO annot key

			if not ids.has_key(newAnnotKey):
				ids[newAnnotKey] = [ tpl ]
			elif not tpl in ids[newAnnotKey]:
				ids[newAnnotKey].append(tpl)

		logger.debug ('Found inferred-from IDs for %d records' % \
			len(ids))

		self.results[3] = (cols, [])
		del rows
		gc.collect()
		logger.debug ('Freed memory from result set 3')

		return ids

	def getSortingMaps (self):
		byVocab = {}
		byAnnotType = {}
		byTermAlpha = {}

		toDo = [ (byVocab, 6), (byAnnotType, 7), (byTermAlpha, 8) ]

		for (d, num) in toDo:
			cols, rows = self.results[num]

			keyCol = Gatherer.columnNumber (cols, 'myKey')
			nameCol = Gatherer.columnNumber (cols, 'name')

			i = 0
			for row in rows:
				i = i + 1
				d[row[keyCol]] = i
				d[row[nameCol]] = i

			self.results[num] = (cols, [])
			del rows
			gc.collect()
			logger.debug('Freed memory from result set %d' % num)

		logger.debug ('Loaded %d sorting maps' % len(toDo))
		return byVocab, byAnnotType, byTermAlpha

	def buildQuery9Rows (self, byVocab, byAnnotType, byTermAlpha):
		# build the extra rows from query 9, where we get the set of
		# MP annotations that have been rolled-up to markers

		# see aCols, mCols, and sCols in buildRows() for column order
		# for these three lists, respectively:

		aRows = []
		mRows = []
		sRows = []

		cols, rows = self.results[9]

		termCol = Gatherer.columnNumber (cols, 'term')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
		accIDCol = Gatherer.columnNumber (cols, 'accID')
		vocabKeyCol = Gatherer.columnNumber (cols, '_Vocab_key')
		markerCol = Gatherer.columnNumber (cols, '_Marker_key')
		annotKeyCol = Gatherer.columnNumber (cols, '_Annot_key')
		typeKeyCol = Gatherer.columnNumber (cols, '_AnnotType_key')

		# We only want to keep the first annotation for a given
		# (marker, term) pair, so we need to track what ones we have
		# kept so far:
		done = {}	# (marker key, term ID) -> 1

		# need a key generator that defines a unique annotation to
		# include term ID, marker key, and a special annotation type
		tmsKeyGenerator = \
			AnnotationKeyGenerator.TermMarkerSpecialKeyGenerator()

		annotTypeKey = MP_MARKER
		annotType = 'Mammalian Phenotype/Marker'

		for row in rows:
			annotKey = self.translateAnnotKey(row[annotKeyCol])

			# if we need to skip this, just move on
			# (should not happen here, but we add it just in case)
			if annotKeyToSkip.has_key(annotKey):
				continue

			markerKey = row[markerCol]
			termID = row[accIDCol]
			pair = (markerKey, termID)

			if done.has_key(pair):
				continue
			done[pair] = 1

			termKey = row[termKeyCol]
			vocabKey = row[vocabKeyCol]
			vocab = Gatherer.resolve (vocabKey, 'voc_vocab',
				'_Vocab_key', 'name')

			annotationKey = tmsKeyGenerator.getKey (annotKey,
				termID = termID,
				markerKey = markerKey,
				specialType = 'MP/Marker')

			if done.has_key(annotationKey):
				continue
			done[annotationKey] = 1

			aRow = [ annotationKey, None, None, vocab,
				row[termCol], termID, termKey, None, None,
				'Marker', annotType, 0, 0 ]
			aRows.append (aRow)

			mRow = [ len(mRows), markerKey, annotationKey,
				None, None, annotType ]
			mRows.append (mRow)

			vdt = VocabSorter.getVocabDagTermSequenceNum(termKey)

			sRow = [ annotationKey,
				VocabSorter.getSequenceNum(termKey),
				byTermAlpha[termKey],
				byVocab[vocabKey],
				0,
				vdt,
				]
			sRows.append (sRow)

			self.byObject.append ( (annotationKey, 'Marker',
				getMarkerSeqNum(markerKey), vdt) )

		logger.debug ('Pulled %d MP terms up to markers' % \
			len(aRows) )

		self.results[9] = (cols, [])
		del rows
		gc.collect()
		logger.debug ('Freed memory from result set 9')

		return aRows, mRows, sRows

	def buildQuery10Rows (self, aRows, mRows, sRows, byVocab, byAnnotType,
			byTermAlpha):
		# build the extra rows from query 10, where we pull Protein
		# Ontology IDs associated with markers up to be annotations

		# see aCols, mCols, and sCols in buildRows() for column order
		# for these three lists, respectively:

		cols, rows = self.results[10]

		termCol = Gatherer.columnNumber (cols, 'term')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
		accIDCol = Gatherer.columnNumber (cols, 'accID')
		vocabKeyCol = Gatherer.columnNumber (cols, '_Vocab_key')
		markerCol = Gatherer.columnNumber (cols, '_Marker_key')
		accKeyCol = Gatherer.columnNumber (cols, '_Accession_key')

		# need a key generator that accounts for accession keys,
		# term IDs, marker keys, and a special annotation type
		atmsKeyGenerator = AnnotationKeyGenerator.AccessionTermMarkerSpecialKeyGenerator()

		for row in rows:
			markerKey = row[markerCol]
			termID = row[accIDCol]
			termKey = row[termKeyCol]
			vocabKey = row[vocabKeyCol]
			vocab = Gatherer.resolve (vocabKey, 'voc_vocab',
				'_Vocab_key', 'name')
			accKey = row[accKeyCol]

#			annotationKey = getNewAnnotationKey (accKey,
#				termID, markerKey, 'PRO/Marker')

			annotationKey = atmsKeyGenerator.getKey (None,
				accessionKey = accKey,
				termID = termID,
				markerKey = markerKey,
				specialType = 'PRO/Marker')

			annotType = 'Protein Ontology/Marker'

			aRow = [ annotationKey, None, None, vocab,
				row[termCol], termID, termKey,
				None, None, 'Marker',
				annotType, 0, 0 ]
			aRows.append (aRow)

			mRow = [ len(mRows), markerKey, annotationKey,
				None, None, annotType ]
			mRows.append (mRow)

			vdt = VocabSorter.getVocabDagTermSequenceNum(termKey)

			sRow = [ annotationKey,
				VocabSorter.getSequenceNum(termKey),
				byTermAlpha[termKey],
				byVocab[vocabKey],
				999,
				vdt,
				]
			sRows.append (sRow)

			self.byObject.append ( (annotationKey, 'Marker',
				getMarkerSeqNum(markerKey), vdt) )

		logger.debug ('Pulled in %d Protein Ontology terms' % \
			len(rows) )

		self.results[10] = (cols, [])
		del rows
		gc.collect()
		logger.debug('Freed memory from result set 10')

		return aRows, mRows, sRows

	def buildQuery12Rows (self, aRows, mRows, sRows,
			byVocab, byAnnotType, byTermAlpha):
		# build the extra rows from query 12, where we pull a set of
		# of derived OMIM annotations to mouse markers

		# see aCols, mCols, and sCols in buildRows() for column order
		# for these three lists, respectively:

		cols, rows = self.results[12]

		termCol = Gatherer.columnNumber (cols, 'term')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
		accIDCol = Gatherer.columnNumber (cols, 'accID')
		vocabKeyCol = Gatherer.columnNumber (cols, '_Vocab_key')
		markerCol = Gatherer.columnNumber (cols, '_Object_key')
		annotKeyCol = Gatherer.columnNumber (cols, '_Annot_key')

		annotTypeKey = OMIM_MARKER
		annotType = 'OMIM/Marker'

		# We only want to keep the first annotation for a given
		# (marker, term) pair, so we need to track what ones we have
		# kept so far:
		done = {}	# (marker key, term ID) -> 1

		# need a key generator that takes into account the term ID,
		# marker key, and a special annotation type
		tmsKeyGenerator = \
			AnnotationKeyGenerator.TermMarkerSpecialKeyGenerator()

		for row in rows:
			annotKey = self.translateAnnotKey(row[annotKeyCol])

			# if we need to skip this, just move on
			# (should not happen here, but we add it just in case)
			if annotKeyToSkip.has_key(annotKey):
				continue

			markerKey = row[markerCol]
			termID = row[accIDCol]
			pair = (markerKey, termID)

			if done.has_key(pair):
				continue
			done[pair] = 1

			termKey = row[termKeyCol]
			vocabKey = row[vocabKeyCol]
			vocab = Gatherer.resolve (vocabKey, 'voc_vocab',
				'_Vocab_key', 'name')

			# use termID and markerKey to distinguish genotypes
			# with multiple allele pairs and different markers

			annotationKey = tmsKeyGenerator.getKey (annotKey,
				termID = termID,
				markerKey = markerKey,
				specialType = annotType)

			if done.has_key(annotationKey):
				continue
			done[annotationKey] = 1

			aRow = [ annotationKey, None, None, vocab,
				row[termCol], termID, termKey,
				None, None, 'Marker',
				annotType, 0, 0 ]
			aRows.append (aRow)

			mRow = [ len(mRows), markerKey, annotationKey,
				None, None, annotType ]
			mRows.append (mRow)

			vdt = VocabSorter.getVocabDagTermSequenceNum(termKey)

			sRow = [ annotationKey,
				VocabSorter.getSequenceNum(termKey),
				byTermAlpha[termKey],
				byVocab[vocabKey],
				0,
				vdt,
				]
			sRows.append (sRow)

			self.byObject.append ( (annotationKey, 'Marker',
				getMarkerSeqNum(markerKey), vdt) )

		logger.debug ('Pulled %d OMIM terms up to markers' % \
			len(aRows) )

		self.results[12] = (cols, [])
		del rows
		gc.collect()
		logger.debug ('Freed memory from result set 12')

		return aRows, mRows, sRows

	def cacheAnnotationData (self):
		# our base data is in the results from query 5

		global annotKeyToSkip

		cols, rows = self.results[5]

		annotCol = Gatherer.columnNumber (cols, '_Annot_key')
		objectCol = Gatherer.columnNumber (cols, '_Object_key')
		qualifierCol = Gatherer.columnNumber (cols, '_Qualifier_key')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
		annotTypeKeyCol = Gatherer.columnNumber (cols,
			'_AnnotType_key')
		objectTypeCol = Gatherer.columnNumber (cols, '_MGIType_key')

		for row in rows:
			# for GO annotations (type 1000), we need to check
			# that we should keep it

			annotKey = self.translateAnnotKey(row[annotCol])
			annotType = row[annotTypeKeyCol]

			if annotType == 1000:
				if not GOFilter.shouldInclude(annotKey):
					annotKeyToSkip[annotKey] = 1
					continue

			AnnotationKeyGenerator.cacheAnnotationAttributes (
				annotKey,
				row[annotTypeKeyCol], row[objectCol],
				row[termKeyCol], row[qualifierCol])

		logger.debug ('Cached data for %d annotations' % len(rows))
		logger.debug ('Skipped %d GO ND annotations' % \
			len(annotKeyToSkip))
		return

	def filterInvalidMarkers (self, mRows):
		# filter out any rows from mRows where are for invalid marker
		# keys
		cols, rows = self.results[11]

		validKeys = {}
		for row in rows:
			validKeys[row[0]] = 1

		i = len(mRows) - 1
		ct = 0

		while i >= 0:
			markerKey = mRows[i][1]
			if not validKeys.has_key(markerKey):
				del mRows[i]
				ct = ct + 1
			i = i - 1

		logger.debug ('Filtered out %d marker annotation rows' % ct) 

		self.results[11] = (cols, [])
		del rows
		gc.collect()
		logger.debug ('Freed memory for result set 11')

		return mRows

	def buildRows (self, termKeyToID, termKeyToDagKey, mgdToNewKeys,
		annotationEvidence, annotationRefs, inferredFromIDs):

		# We will produe rows for four tables on the first pass:

		# annotation table columns and rows
		aCols = [ 'annotation_key', 'dag_name', 'qualifier',
			'vocab_name', 'term', 'term_id', 'term_key',
			'evidence_code', 'evidence_term',
			'object_type', 'annotation_type', 'reference_count',
			'inferred_id_count' ]
		aRows = []

		# annotation_inferred_from_id columns and rows
		iCols = [ 'unique_key', 'annotation_key', 'logical_db',
			'acc_id', 'organism', 'preferred', 'private',
			'sequence_num' ]
		iRows = []

		# annotation_reference columns and rows
		rCols = [ 'unique_key', 'annotation_key', 'reference_key',
			'jnum_id', 'sequence_num' ]
		rRows = []

		# marker_to_annotation columns and rows
		mCols = [ 'unique_key', 'marker_key', 'annotation_key',
			'reference_key', 'qualifier', 'annotation_type' ]
		mRows = []

		# genotype_to_annotation columns and rows
		gCols = [ 'unique_key', 'genotype_key', 'annotation_key',
			'reference_key', 'qualifier', 'annotation_type' ]
		gRows = []

		# annotation_sequence_num columns and rows
		sCols = [ 'annotation_key', 'by_dag_structure',
			'by_term_alpha', 'by_vocab', 'by_annotation_type',
			'by_vocab_dag_term', 'by_marker_dag_term' ]
		sRows = []

		# annotation_to_header columns and rows
		hCols = [ 'annotation_key', 'header_term_key' ]
		hRows = []

		finder = OrganismFinder()

		# object/dag/term sorting to be done later; just collect the
		# needed data for now.  We need to put this in an instance
		# variable so other methods can populate it as well. Contains:
		# [ (annot key, object type, object seq num,
		#    vocab/dag/term seq num), ... ]
		self.byObject = []

		# get our sorting maps for vocab and annotation type
		byVocab, byAnnotType, byTermAlpha = self.getSortingMaps()

		# get the starter data for markers that we're pulling up from
		# MP/genotype annotations

		aRows, mRows, sRows = self.buildQuery9Rows(byVocab,
			byAnnotType, byTermAlpha)

		# fill in the Protein Ontology data
		aRows, mRows, sRows = self.buildQuery10Rows (aRows, mRows,
			sRows, byVocab, byAnnotType, byTermAlpha)

		# fill in the OMIM data (pulled up from genotypes to markers)
		aRows, mRows, sRows = self.buildQuery12Rows (aRows, mRows,
			sRows, byVocab, byAnnotType, byTermAlpha)

		# our base data is in the results from query 5
		cols, rows = self.results[5]

		annotCol = Gatherer.columnNumber (cols, '_Annot_key')
		objectCol = Gatherer.columnNumber (cols, '_Object_key')
		qualifierCol = Gatherer.columnNumber (cols, '_Qualifier_key')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
		vocabKeyCol = Gatherer.columnNumber (cols, '_Vocab_key')
		annotTypeCol = Gatherer.columnNumber (cols, 'annotType')
		annotTypeKeyCol = Gatherer.columnNumber (cols,
			'_AnnotType_key')
		termCol = Gatherer.columnNumber (cols, 'term')
		objectTypeCol = Gatherer.columnNumber (cols, '_MGIType_key')

		# new annot key -> 1 (once done)
		done = {}

		# need a key generator that only considers the annotation key
		# and the special 'no evidence' type
		sKeyGenerator = AnnotationKeyGenerator.SpecialKeyGenerator()

		for row in rows:
			# base values that we'll need later
			annotKey = self.translateAnnotKey(row[annotCol])

			# if we need to skip this, just move on
			if annotKeyToSkip.has_key(annotKey):
				continue

			mgiType = row[objectTypeCol]
			objectKey = row[objectCol]
			termKey = row[termKeyCol]
			term = row[termCol]
			annotType = row[annotTypeCol]
			termID = None
			vocabKey = row[vocabKeyCol]
			annotTypeKey = row[annotTypeKeyCol]

			if termKeyToID.has_key(termKey):
				termID = termKeyToID[termKey]

			# resolve the keys to their respective terms
			qualifier = Gatherer.resolve (row[qualifierCol])
			objectType = Gatherer.resolve (row[objectTypeCol],
				'ACC_MGIType', '_MGIType_key', 'name')
			vocab = Gatherer.resolve (vocabKey,
				'VOC_Vocab', '_Vocab_key', 'name')

			dagName = None	# name of the term's DAG

			if termKeyToDagKey.has_key(termKey):
				dagKey = termKeyToDagKey[termKey]
				if dagKey:
					dagName = Gatherer.resolve (dagKey,
						'DAG_DAG', '_DAG_key', 'name')

			# list of annotation records produced for this
			# object for this mgd annotation record
			annotations = []

			# for annotations without evidence, they would not
			# have been assigned a new annotation key for the
			# front-end database; get one now
			if not mgdToNewKeys.has_key(annotKey):
				mgdToNewKeys[annotKey] = [
				    sKeyGenerator.getKey(annotKey,
					    specialType = 'no evidence') ]

			for annotationKey in mgdToNewKeys[annotKey]:
			    refCount = 0	# number of references
			    idCount = 0		# number of inferred-from IDs
			    evidenceCode = None	# default to no evidence code
			    evidenceTerm = None	# default to no evidence code

			    if done.has_key(annotationKey):
				    continue
			    done[annotationKey] = 1

			    # look up an evidence code, if one exists
			    if annotationEvidence.has_key(annotationKey):
				evidenceCode = Gatherer.resolve (
				    annotationEvidence[annotationKey],
				    'VOC_Term', '_Term_key', 'abbreviation')
				evidenceTerm = Gatherer.resolve (
				    annotationEvidence[annotationKey],
				    'VOC_Term', '_Term_key', 'term')

			    # reference records
			    if annotationRefs.has_key(annotationKey):
				refs = annotationRefs[annotationKey]
				for (refsKey, jnum) in refs:
				    rRows.append ( (len(rRows),
					annotationKey, refsKey, jnum,
					len(rRows) ) )
				refCount = len(annotationRefs[annotationKey])

			    # inferred-from IDs
			    if inferredFromIDs.has_key(annotationKey):
				ids = inferredFromIDs[annotationKey]
				for (id, ldb, preferred) in ids:
				    iRows.append ( (len(iRows),
					annotationKey, ldb, id,
					finder.getOrganism(id),
					preferred, 0, len(iRows) ) )
				idCount = len(inferredFromIDs[annotationKey])

			    # populate annotation table

			    baseRow = [ annotationKey, dagName, qualifier,
				vocab, term, termID, termKey,
				evidenceCode, evidenceTerm,
				objectType, annotType, refCount, idCount ]

			    aRows.append (baseRow)

			    # populate the mapping to headers, if needed

			    if self.headers.has_key(termKey):
				    for hKey in self.headers[termKey]:
					    hRows.append((annotationKey, hKey))

			    # populate annotation_sequence_num table

			    vdt = VocabSorter.getVocabDagTermSequenceNum(
				termKey)

			    sRow = [ annotationKey,
				VocabSorter.getSequenceNum(termKey),
				byTermAlpha[termKey],
				byVocab[vocabKey],
				byAnnotType[annotTypeKey],
				vdt,
				]
			    sRows.append (sRow)
			
			    if mgiType == MARKER:
				self.byObject.append ( (annotationKey,
				    objectType,
				    getMarkerSeqNum(objectKey), vdt) )
			    elif mgiType == GENOTYPE:
				self.byObject.append ( (annotationKey,
				    objectType,
				    AlleleAndGenotypeSorter.getGenotypeSequenceNum(objectKey),
				    vdt) )
			    else:
				self.byObject.append ( (annotationKey,
				    objectType,
				    self.maxMarkerSequenceNum + 1, vdt) )

			    # remember this annotation for later use

			    annotations.append ( (annotationKey, qualifier,
				    annotType) )

			# use the annotations to populate the necessary join
			# tables to other objects

		 	if row[objectTypeCol] == MARKER:
			    for (annotationKey, qualifier, annotType) in \
				annotations:

				mRows.append ( (len(mRows), objectKey,
				    annotationKey, None, qualifier,
				    annotType) )

			elif row[objectTypeCol] == GENOTYPE:
			    for (annotationKey, qualifier, annotType) in \
				annotations:

				gRows.append ( (len(gRows), objectKey,
				    annotationKey, None, qualifier,
				    annotType) )

		logger.debug ('Collected basic data')

		mRows = self.filterInvalidMarkers(mRows)

		logger.debug ('Filtered invalid markers')

		# now we need to go back, compute, and append to 'sRows' the
		# sorting by object and vocab/dag/term for each annotation;
		# sort by object type, object nomen, then vocab/dag/term, then
		# annot key

		self.byObject.sort (lambda a, b : cmp(
			(a[1], a[2], a[3], a[0]),
			(b[1], b[2], b[3], b[0]) ) )

		i = 0
		byAnnotation = {}
		for (annotationKey, objectType, objectSeqNum, termSeqNum) in self.byObject:
			i = i + 1
			byAnnotation[annotationKey] = i 

		for sRow in sRows:
			sRow.append(byAnnotation[sRow[0]])

		logger.debug ('Appended extra marker/dag sort')

		self.output = [ (aCols, aRows), (iCols, iRows),
			(rCols, rRows), (mCols, mRows), (sCols, sRows),
			(gCols, gRows), (hCols, hRows) ]
		logger.debug ('%d annotations, %d IDs, %d refs, %d markers' \
			% (len(aRows), len(iRows), len(rRows), len(mRows) ) )
		return

	def collateResults (self):
		# build annotation consolidator
		self.buildAnnotationConsolidator()

		# cache marker data
		self.buildMarkerCache()

		# cache some of our base annotation data for use later
		self.cacheAnnotationData()

		# process query 0 - maps term key to primary ID
		termKeyToID = self.getTermIDs()

		# process query 1 - maps term key to DAG key
		termKeyToDagKey = self.getDags()

		# process query 2 - evidence for annotations
		mgdToNewKeys, annotationEvidence, annotationRefs = \
			self.getEvidence()

		# process queries 3 and 4 - inferred-from IDs
		inferredFromIDs = self.getInferredFromIDs()

		# process query 15 - maps term key to its header term keys
		self.cacheHeaderTerms()

		# process query 5 and join with prior results
		self.buildRows (termKeyToID, termKeyToDagKey, mgdToNewKeys,
			annotationEvidence, annotationRefs, inferredFromIDs)
		return

###--- globals ---###

cmds = [
	# Note that we're excluding the two derived annotation types from most
	# of these queries, as they're handled specially.  (This ensures that
	# NOTs are removed.  If we want all the derived annotations available
	# here -- with their original annotation types -- then we just need to
	# remove the restrictions.)

	# 0. vocab terms' primary IDs
	'''select _Object_key,
		accID
	from acc_accession
	where _MGIType_key = 13
		and private = 0
		and preferred = 1''',

	# 1. DAG key for each vocab term
	'''select va._Term_key,
		dn._DAG_key
	from voc_annot va,
		dag_node dn
	where va._Term_key = dn._Object_key''',

	# 2. evidence for each annotation
	'''select ve._Annot_key,
		ve._AnnotEvidence_key,
		bc.jnumID,
		bc._Refs_key,
		bc.numericPart,
		ve.inferredFrom,
		ve._EvidenceTerm_key
	from voc_evidence ve,
		bib_citation_cache bc,
		voc_annot va
	where ve._Refs_key = bc._Refs_key
		and ve._Annot_key = va._Annot_key
		and va._AnnotType_key not in (%d, %d)
	order by ve._Annot_key, bc.numericPart''' % (OMIM_MARKER, MP_MARKER),
		
	# 3. 'inferred from' IDs for each annotation/evidence pair
	'''select ve._Annot_key,
		ve._AnnotEvidence_key,
		aa.accID,
		aa._LogicalDB_key,
		aa.prefixPart,
		aa.numericPart,
		aa.preferred,
		ldb.name as logical_db,
		ve.inferredFrom,
		ve._EvidenceTerm_key
	from voc_evidence ve,
		acc_accession aa,
		acc_logicaldb ldb,
		voc_annot va
	where ve._AnnotEvidence_key = aa._Object_key
		and aa._MGIType_key = 25
		and aa._LogicalDB_key = ldb._LogicalDB_key
		and ve._Annot_key = va._Annot_key
		and va._AnnotType_key not in (%d, %d)
	order by ve._AnnotEvidence_key, aa.prefixPart, aa.numericPart''' % (
		OMIM_MARKER, MP_MARKER),

	# 4. get the set of IDs which are sequence IDs from NCBI, EMBL, and
	# UniProt; for these, we will need to replace the logical database
	# with the MGI logical database to link to our sequence detail page
	'''select distinct aa.accID,
		aa._LogicalDB_key
	from acc_accession aa, %s aa2
	where aa._MGIType_key = 19
		and aa._LogicalDB_key in (68, 9, 13)
		and aa.accID = aa2.accID''' % EVIDENCE_ID_TABLE,

	# 5. get all annotations
	'''select va._Object_key,
		va._Annot_key,
		va._Qualifier_key,
		va._Term_key,
		vat._AnnotType_key,
		vat.name as annotType,
		vt.term,
		vat._MGIType_key,
		vt._Vocab_key
	from voc_annot va,
		voc_annottype vat,
		voc_term vt
	where va._AnnotType_key = vat._AnnotType_key
		and va._Term_key = vt._Term_key
		and va._AnnotType_key not in (%d, %d)
	order by _Object_key''' % (OMIM_MARKER, MP_MARKER),

	# 6. get all vocabularies
	'''select _Vocab_key as myKey,
		name 
	from voc_vocab 
	order by name''',

	# 7. get all annotation types
	'''select _AnnotType_key as myKey,
		name 
	from voc_annottype
	order by name''',

	# 8. get all terms
	'''select _Term_key as myKey,
		term as name
	from voc_term
	order by term''',

	# 9. get the set of MP annotations that have been rolled up to markers
	# (only keep null qualifiers, to avoid NOT and "normal" annotations).
	'''select distinct va._Annot_key,
		va._Term_key,
		t.term,
		t._Vocab_key,
		a.accID,
		va._Object_key as _Marker_key,
		va._AnnotType_key
	from voc_annot va,
		voc_term t,
		acc_accession a,
		voc_term q
	where va._AnnotType_key = %d
		and va._Term_key = t._Term_key
		and va._Term_key = a._Object_key
		and a._MGIType_key = 13
		and a.preferred = 1
		and va._Qualifier_key = q._Term_key
		and q.term is null''' % MP_MARKER,

	# 10. get the Protein Ontology IDs for each marker, so we can convert
	# them to be annotations
	'''select distinct a._Accession_key,
		t._Term_key,
		t.term,
		a.accID,
		t._Vocab_key,
		a._Object_key as _Marker_key
	from acc_accession a,
		voc_term t,
		acc_accession a2
	where a._MGIType_key = 2
		and a._LogicalDB_key = 135
		and a.private = 0
		and a.accID = a2.accID
		and a2._MGIType_key = 13
		and a2._Object_key = t._Term_key''',

	# 11. get the valid marker keys
	'''select _Marker_key from mrk_marker''',

	# 12. get OMIM annotations that have been derived for mouse markers
	# via a series of rollup rules in the production database.  (These
	# annotations are made to genotypes and the rollup rules determine
	# when they should be tied to a specific marker.)
	# Exclude: annotations with a NOT qualifier
	'''select va._Annot_key,
		va._Term_key,
		t.term,
		t._Vocab_key,
		a.accID,
		va._Object_key
	from voc_annot va,
		voc_term t,
		acc_accession a
	where va._AnnotType_key = %d
		and va._Term_key = t._Term_key
		and va._Term_key = a._Object_key
		and a._MGIType_key = 13
		and a.preferred = 1
		and va._Qualifier_key != %d''' % (
			OMIM_MARKER, NOT_QUALIFIER),

	# 13. get a set of basic data about markers so we can cache the marker
	# data for each annotation (order by logical db key so MGI IDs come
	# first and are preferred)
	'''select m._Marker_key, m.symbol, m.name, m.chromosome,
		ldb._LogicalDB_key, ldb.name as logical_db, a.accID
	from mrk_marker m,
		acc_accession a,
		acc_logicaldb ldb
	where m._Marker_key = a._Object_key
		and a._MGIType_key = 2
		and a.private = 0
		and a.preferred = 1
		and a._LogicalDB_key = ldb._LogicalDB_key
	order by 1, 4''',

	# 14. get a basic set of data about annotations, so we can consolidate
	# into one all the disparate annotations that should really be
	# considered a single annotation (based on matching annotation type,
	# object key, term key, qualifier key)
	'''select _Annot_key, _AnnotType_key, _Term_key, _Object_key,
		_Qualifier_key
	from voc_annot''',

	# 15. mapping from GO terms to the keys of the header terms to which
	# they can be aggregated.  (Only GO currently, because that's the only
	# need.  If we need MP at a later date, switch to an IN and add it.)
	'''select t._Term_key as header_term_key,
		t._Term_key as term_key
	from voc_term t
	where t._Vocab_key = 4
		and t.abbreviation is not null
		and t.sequenceNum is not null
	union
	select h._Term_key as header_term_key, t._Term_key as term_key
	from voc_term h, dag_closure dc, voc_term t
	where h._Vocab_key = 4
		and h.abbreviation is not null
		and h.sequenceNum is not null
		and h._Term_key = dc._AncestorObject_key
		and dc._DescendentObject_key = t._Term_key''',
	]

# definition of output files, each as:
#	(filename prefix, list of fieldnames, table name)
files = [
	('annotation',
		[ 'annotation_key', 'dag_name', 'qualifier', 'vocab_name',
			'term', 'term_id', 'term_key',
			'evidence_code', 'evidence_term',
			'object_type', 'annotation_type', 'reference_count',
			'inferred_id_count' ],
		'annotation'),

	('annotation_inferred_from_id',
		[ 'unique_key', 'annotation_key', 'logical_db', 'acc_id',
			'organism', 'preferred', 'private', 'sequence_num' ],
		'annotation_inferred_from_id'),

	('annotation_reference',
		[ 'unique_key', 'annotation_key', 'reference_key',
			'jnum_id', 'sequence_num' ],
		'annotation_reference'),

	('marker_to_annotation',
		[ 'unique_key', 'marker_key', 'annotation_key',
			'reference_key', 'qualifier', 'annotation_type' ],
		'marker_to_annotation'),

	('annotation_sequence_num',
		[ 'annotation_key', 'by_dag_structure', 'by_term_alpha',
			'by_vocab', 'by_annotation_type', 'by_vocab_dag_term',
			'by_marker_dag_term',
			],
		'annotation_sequence_num'),

	('genotype_to_annotation',
		[ 'unique_key', 'genotype_key', 'annotation_key',
			'reference_key', 'qualifier', 'annotation_type' ],
		'genotype_to_annotation'),

	('annotation_to_header',
		[ Gatherer.AUTO, 'annotation_key', 'header_term_key' ],
		'annotation_to_header'),
	]

# global instance of a AnnotationGatherer
gatherer = AnnotationGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	initialize()
	Gatherer.main (gatherer)
