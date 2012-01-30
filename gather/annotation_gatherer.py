#!/usr/local/bin/python
# 
# gathers data for the 'annotation_*' and '*_to_annotation' tables in the
# front-end database

import Gatherer
import VocabSorter
import logger

###--- Constants ---###

MARKER = 2		# MGI Type for markers

###--- Functions ---###

# annotKey -> (annotTypeKey, objectKey, termKey, qualifierKey)
annotKeyAttributes = {}

def putAnnotAttributes (annotKey, annotType, objectKey, termKey, qualifier):
	global annotKeyAttributes

	annotKeyAttributes[annotKey] = (annotType, objectKey, termKey,
		qualifier)
	return

def getAnnotAttributes (annotKey):
	# returns (annot type, object key, term key, qualifier) if available,
	#	or None

	if annotKeyAttributes.has_key(annotKey):
		return annotKeyAttributes[annotKey]
	return None

newAnnotKeys = {}

def getNewAnnotationKey (annotKey, evidenceTermKey, inferredFrom):
	# look up (or generate) a "new annotation key" to identify the
	# annotation described by the input parameters

	global newAnnotKeys

	attributes = getAnnotAttributes(annotKey)
	if attributes:
		(annotType, objectKey, termKey, qualifier) = attributes
		tpl = (annotType, objectKey, termKey, qualifier,
			evidenceTermKey, inferredFrom)
	else:
		logger.debug ('Unknown annotation key: %d' % annotKey)
		tpl = (annotKey, evidenceTermKey, inferredFrom)

	if not newAnnotKeys.has_key(tpl):
		newAnnotKeys[tpl] = len(newAnnotKeys) + 1
	return newAnnotKeys[tpl]

###--- Classes ---###

class AnnotationGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for most of the marker_go_* tables
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for markers' GO
	#	annotations, collates results, writes tab-delimited text files

	def getTermIDs (self):
		cols, rows = self.results[0]
		keyCol = Gatherer.columnNumber (cols, '_Object_key')
		idCol = Gatherer.columnNumber (cols, 'accID')

		termKeyToID = {}
		for row in rows:
			termKeyToID[row[keyCol]] = row[idCol]

		logger.debug ('Found %d term IDs' % len(termKeyToID))
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
			annotKey = row[annotKeyCol]
			evidenceTermKey = row[evidenceTermCol]
			inferredFrom = row[inferredFromCol]

			newKey = getNewAnnotationKey (annotKey,
				evidenceTermKey, inferredFrom)

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
			annotKey = row[akeyCol]
			evidence = row[evidenceCol]
			id = row[idCol]
			ldbKey = row[ldbKeyCol]
			ldbName = row[ldbCol]

			# get the special key we are generating for each
			# unique (annotation key, evidence term key,
			# inferred from) tuple

			newAnnotKey = getNewAnnotationKey (annotKey,
				evidence, row[inferredCol])

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

		logger.debug ('Loaded %d sorting maps' % len(toDo))
		return byVocab, byAnnotType, byTermAlpha

	def buildQuery9Rows (self, byVocab, byAnnotType, byTermAlpha,
		musHumOrtho):
		# build the extra rows from query 9, where we pull a summary
		# of annotations up from genotypes through alleles to markers

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

		for row in rows:
			markerKey = row[markerCol]
			termID = row[accIDCol]
			pair = (markerKey, termID)
			annotKey = row[annotKeyCol]

			if done.has_key(pair):
				continue
			done[pair] = 1

			termKey = row[termKeyCol]
			vocabKey = row[vocabKeyCol]
			vocab = Gatherer.resolve (vocabKey, 'voc_vocab',
				'_Vocab_key', 'name')

			# use termID and markerKey to distinguish genotypes
			# with multiple allele pairs and different markers
			annotationKey = getNewAnnotationKey (annotKey, 
				termID, markerKey)

			if done.has_key(annotationKey):
				continue
			done[annotationKey] = 1

			annotTypeKey = row[typeKeyCol]
			annotType = Gatherer.resolve (row[typeKeyCol],
				'voc_annottype', '_AnnotType_key', 'name')
			annotType = annotType.replace ('Genotype', 'Marker')

			# We only want to annotate OMIM terms to markers which
			# have human orthologs.
			if annotType == 'OMIM/Marker':
				if not musHumOrtho.has_key(markerKey):
					continue

			aRow = [ annotationKey, None, None, vocab,
				row[termCol], termID, None, 'Marker',
				annotType, 0, 0 ]
			aRows.append (aRow)

			mRow = [ len(mRows), markerKey, annotationKey,
				None, None, annotType ]
			mRows.append (mRow)

			sRow = [ annotationKey,
				VocabSorter.getSequenceNum(termKey),
				byTermAlpha[termKey],
				byVocab[vocabKey],
				0 ]
			sRows.append (sRow)

		logger.debug ('Pulled %d OMIM/MP terms up to markers' % \
			len(aRows) )
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

		for row in rows:
			markerKey = row[markerCol]
			termID = row[accIDCol]
			termKey = row[termKeyCol]
			vocabKey = row[vocabKeyCol]
			vocab = Gatherer.resolve (vocabKey, 'voc_vocab',
				'_Vocab_key', 'name')
			accKey = row[accKeyCol]

			annotationKey = getNewAnnotationKey (accKey,
				termID, markerKey)

			annotType = 'Protein Ontology/Marker'

			aRow = [ annotationKey, None, None, vocab,
				row[termCol], termID, None, 'Marker',
				annotType, 0, 0 ]
			aRows.append (aRow)

			mRow = [ len(mRows), markerKey, annotationKey,
				None, None, annotType ]
			mRows.append (mRow)

			sRow = [ annotationKey,
				VocabSorter.getSequenceNum(termKey),
				byTermAlpha[termKey],
				byVocab[vocabKey],
				999
				]
			sRows.append (sRow)

		logger.debug ('Pulled in %d Protein Ontology terms' % \
			len(rows) )

		return aRows, mRows, sRows

	def cacheAnnotationData (self):
		# our base data is in the results from query 5
		cols, rows = self.results[5]

		annotCol = Gatherer.columnNumber (cols, '_Annot_key')
		objectCol = Gatherer.columnNumber (cols, '_Object_key')
		qualifierCol = Gatherer.columnNumber (cols, '_Qualifier_key')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
		annotTypeKeyCol = Gatherer.columnNumber (cols,
			'_AnnotType_key')
		objectTypeCol = Gatherer.columnNumber (cols, '_MGIType_key')

		for row in rows:
			putAnnotAttributes (row[annotCol],
				row[annotTypeKeyCol], row[objectCol],
				row[termKeyCol], row[qualifierCol])

		logger.debug ('Cached data for %d annotations' % len(rows))
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
		return mRows

	def buildRows (self, termKeyToID, termKeyToDagKey, mgdToNewKeys,
		annotationEvidence, annotationRefs, inferredFromIDs,
		musHumOrtho):

		# We will produe rows for four tables on the first pass:

		# annotation table columns and rows
		aCols = [ 'annotation_key', 'dag_name', 'qualifier',
			'vocab_name', 'term', 'term_id', 'evidence_code',
			'object_type', 'annotation_type', 'reference_count',
			'inferred_id_count' ]
		aRows = []

		# annotation_inferred_from_id columns and rows
		iCols = [ 'unique_key', 'annotation_key', 'logical_db',
			'acc_id', 'preferred', 'private', 'sequence_num' ]
		iRows = []

		# annotation_reference columns and rows
		rCols = [ 'unique_key', 'annotation_key', 'reference_key',
			'jnum_id', 'sequence_num' ]
		rRows = []

		# marker_to_annotation columns and rows
		mCols = [ 'unique_key', 'marker_key', 'annotation_key',
			'reference_key', 'qualifier', 'annotation_type' ]
		mRows = []

		# annotation_sequence_num columns and rows
		sCols = [ 'annotation_key', 'by_dag_structure',
			'by_term_alpha', 'by_vocab', 'by_annotation_type' ]
		sRows = []

		# get our sorting maps for vocab and annotation type
		byVocab, byAnnotType, byTermAlpha = self.getSortingMaps()

		# get the starter data for markers that we're pulling up from
		# genotype annotations

		aRows, mRows, sRows = self.buildQuery9Rows(byVocab,
			byAnnotType, byTermAlpha, musHumOrtho)

		# fill in the Protein Ontology data
		aRows, mRows, sRows = self.buildQuery10Rows (aRows, mRows,
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

		for row in rows:
			# base values that we'll need later
			annotKey = row[annotCol]
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
				    getNewAnnotationKey (
					annotKey, None, None) ]

			for annotationKey in mgdToNewKeys[annotKey]:
			    refCount = 0	# number of references
			    idCount = 0		# number of inferred-from IDs
			    evidenceCode = None	# default to no evidence code

			    if done.has_key(annotationKey):
				    continue
			    done[annotationKey] = 1

			    # look up an evidence code, if one exists
			    if annotationEvidence.has_key(annotationKey):
				evidenceCode = Gatherer.resolve (
				    annotationEvidence[annotationKey],
				    'VOC_Term', '_Term_key', 'abbreviation')

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
					annotationKey, ldb, id, preferred,
					0, len(iRows) ) )
				idCount = len(inferredFromIDs[annotationKey])

			    # populate annotation table

			    baseRow = [ annotationKey, dagName, qualifier,
				vocab, term, termID, evidenceCode, objectType,
				annotType, refCount, idCount ]

			    aRows.append (baseRow)

			    # populate annotation_sequence_num table

			    sRow = [ annotationKey,
				VocabSorter.getSequenceNum(termKey),
				byTermAlpha[termKey],
				byVocab[vocabKey],
				byAnnotType[annotTypeKey] ]
			    sRows.append (sRow)

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

		mRows = self.filterInvalidMarkers(mRows)

		self.output = [ (aCols, aRows), (iCols, iRows),
			(rCols, rRows), (mCols, mRows), (sCols, sRows) ]
		logger.debug ('%d annotations, %d IDs, %d refs, %d markers' \
			% (len(aRows), len(iRows), len(rRows), len(mRows) ) )
		return

	def getMouseGenesWithHumanOrthologs(self):
		cols, rows = self.results[12]

		musHumOrtho = {}	# mouse marker key -> 1

		# only one column (marker key)

		for row in rows:
			musHumOrtho[row[0]] = 1

		logger.debug ('%d mouse markers with human orthologs' % \
			len(musHumOrtho))

		return musHumOrtho

	def collateResults (self):
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

		# process query 12 - set of mouse marker keys with human
		# orthologs
		musHumOrtho = self.getMouseGenesWithHumanOrthologs()

		# process query 5 and join with prior results
		self.buildRows (termKeyToID, termKeyToDagKey, mgdToNewKeys,
			annotationEvidence, annotationRefs, inferredFromIDs,
			musHumOrtho)
		return

###--- globals ---###

cmds = [
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
		bib_citation_cache bc
	where ve._Refs_key = bc._Refs_key
	order by ve._AnnotEvidence_key, bc.numericPart''',
		
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
		acc_logicaldb ldb
	where ve._AnnotEvidence_key = aa._Object_key
		and aa._MGIType_key = 25
		and aa._LogicalDB_key = ldb._LogicalDB_key
	order by ve._AnnotEvidence_key, aa.prefixPart, aa.numericPart''',

	# 4. get the set of IDs which are sequence IDs from NCBI, EMBL, and
	# UniProt; for these, we will need to replace the logical database
	# with the MGI logical database to link to our sequence detail page
	'''select distinct aa.accID,
		aa._LogicalDB_key
	from acc_accession aa
	where aa._MGIType_key = 25
		and aa._LogicalDB_key in (68, 9, 13)
		and exists (select 1 from acc_accession aa2
			where aa.accID = aa2.accID
				and aa2._MGIType_key = 19
				and aa2._LogicalDB_key in (68, 9, 13))''',

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
	order by _Object_key''',

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

	# 9. get OMIM and MP annotations made to genotypes, and pull a brief
	# set of info for them up through their alleles to their markers
	# (only null qualifiers, to avoid NOT and "normal" annotations).  The
	# union would also pull in OMIM annotations to orthologous human
	# markers, but is commented-out for the time being.
	'''select distinct va._Annot_key,
		vt._Term_key,
		vt.term,
		aa.accID,
		vt._Vocab_key,
		gag._Marker_key,
		va._AnnotType_key
	from gxd_allelegenotype gag,
		voc_annot va,
		voc_term vt,
		voc_term vq,
		acc_accession aa
	where gag._Genotype_key = va._Object_key
		and va._AnnotType_key in (1005, 1002)
		and va._Term_key = vt._Term_key
		and va._Qualifier_key = vq._Term_key
		and va._Term_key = aa._Object_key
		and aa._MGIType_key = 13
		and aa.preferred = 1
		and gag._Marker_key is not null
		and vq.term is null''',

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

	# 12. get the mouse marker keys which have orthologous human markers
	'''select distinct m._Marker_key
	from mrk_homology_cache m, mrk_homology_cache h
	where m._Class_key = h._Class_key
		and m._Organism_key = 1
		and h._Organism_key = 2''',
	]

# definition of output files, each as:
#	(filename prefix, list of fieldnames, table name)
files = [
	('annotation',
		[ 'annotation_key', 'dag_name', 'qualifier', 'vocab_name',
			'term', 'term_id', 'evidence_code', 'object_type',
			'annotation_type', 'reference_count',
			'inferred_id_count' ],
		'annotation'),

	('annotation_inferred_from_id',
		[ 'unique_key', 'annotation_key', 'logical_db', 'acc_id',
			'preferred', 'private', 'sequence_num' ],
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
			'by_vocab', 'by_annotation_type' ],
		'annotation_sequence_num'),
	]

# global instance of a AnnotationGatherer
gatherer = AnnotationGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
