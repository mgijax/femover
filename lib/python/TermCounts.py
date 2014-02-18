# Module: TermCounts.py
# Purpose: to provide for conversion of any special MGI markup tags that we
#	want to process in the mover (as opposed to in the web products)
# Notes: We are NOT currently handling the anatomical dictionary,
#	as it is not needed for 5.0.


import dbAgnostic
import logger
import types
import re
import gc

###--- Globals ---###

# We now process a single vocabulary at a time, so we need to keep track of
# which vocab is currently cached in memory.
currentVocabKey = None

# term key -> 1
# We need to track all terms in the current vocab, so we'll know when we need
# to update to a new vocab.
termsInCurrentVocab = {}

# term key -> { marker key : 1 }
# only has terms for the current vocab now.
markersPerTerm = {}

# term key -> { marker key : 1 } where those markers are in the
# GXD Literature Index
# only has terms for the current vocab now.
markersInGxdIndex = {}

# term key -> { marker key : 1 } where those markers have expression data
# only has terms for the current vocab now.
markersWithExpressionData = {}

# marker key -> 1 where those markers have Cre data
# only has terms for the current vocab now.
markersWithCreData = None

# have we called _initializeMarkerSets() yet?
initializedMarkerSets = False


###--- Private Functions ---###

def _initializeMarkerSets ():
	global markersWithCreData, markersInGxdIndex
	global markersWithExpressionData, initializedMarkerSets

	if initializedMarkerSets:
		return

	markersWithCreData = {}
	markersWithExpressionData = {}
	markersInGxdIndex = {}

	cmds = [
		# 0. markers with Cre data
		''' select distinct _Marker_key
			from gxd_expression where isrecombinase=1''',

		# 1. markers with full coded expression data
		'''select distinct _Marker_key
		from gxd_expression''',

		# 2. markers in GXD literature index
		'''select distinct _Marker_key
		from gxd_index''',
		]

	# find markers with Cre data
	cols, rows = dbAgnostic.execute(cmds[0])
	for row in rows:
		markersWithCreData[row[0]] = 1

	logger.debug ('Found %d markers with cre data' % \
		len(markersWithCreData))

	# find markers with expression data
	(cols, rows) = dbAgnostic.execute (cmds[1])
	for row in rows:
		markersWithExpressionData[row[0]] = 1

	logger.debug ('Found %d markers with expression data' % \
		len(markersWithExpressionData))

	# find markers in the GXD literature index
	(cols, rows) = dbAgnostic.execute (cmds[2])
	for row in rows:
		markersInGxdIndex[row[0]] = 1

	logger.debug ('Found %d markers in GXD lit index' % \
		len(markersInGxdIndex))

	initializedMarkerSets = True
	return

def _initializeVocab (termKey):
	# initialize this module to include all terms from the vocabulary
	# which contains the given 'termKey'

	global markersPerTerm, currentVocabKey, termsInCurrentVocab

	if termsInCurrentVocab.has_key(termKey):
		# already have this term, so this was called in error
		# (should not happen)
		return

	cmd = 'select _Vocab_key from voc_term where _Term_key = %d' % termKey

	cols, rows = dbAgnostic.execute(cmd)
	if len(rows) == 0:
		# term key does not exist in the database (should not happen)
		return

	vocabKey = rows[0][0]

	if vocabKey == currentVocabKey:
		# term should have been in the currently cached vocab
		return

	# termKey is from a new vocab, so switch over the caches

	logger.debug('Switching to vocab %d' % vocabKey)

	currentVocabKey = vocabKey

	markersPerTerm = {}
	termsInCurrentVocab = {}

	gc.collect()

	# Due to the nature of the DAG, it could be problematic to compute
	# the counts in SQL.  While possible, it would involve heavy use of
	# temp tables.  So, we're going to go the simpler route and just
	# collate the markers for each term and its descendents in code.

	# special cases:
	#	a. no obsolete terms
	#	b. no "normal" annotations for MP terms (term key 2181424)
	#	c. no "not" annotations for GO terms (term keys 1614151,
	#		1614153, 1614155)
	#	d. no "not" annotations for OMIM terms (term key 1614157)
	#		to genotypes
	#	e. no annotations to top-level GO terms (term keys 120, 6112, 
	#		6113, 1098)

	cmds = [
		# 0. mouse markers annotated to the terms themselves
		# (all annotation types which are direct to markers)
		'''select distinct va._Term_key,
			va._Object_key as _Marker_key
		from voc_annot va,
			voc_annottype vat,
			voc_term t,
			mrk_marker m
		where va._AnnotType_key = vat._AnnotType_key
			and va._Term_key = t._Term_key
			and t.isObsolete = 0
			and t._Vocab_key = %d
			and va._Object_key = m._Marker_key
			and va._Qualifier_key not in (1614151, 1614153, 1614155)
			and va._Term_key not in (120, 6112, 6113, 1098)
			and m._Organism_key = 1
			and vat._MGIType_key = 2''' % vocabKey,

		# 1. mouse markers annotated to the terms' descendents
		# (all annotation types which are direct to markers)
		'''select distinct dc._AncestorObject_key as _Term_key,
			va._Object_key as _Marker_key
		from voc_annot va,
			dag_closure dc,
			voc_annottype vat,
			voc_term t,
			mrk_marker m
		where va._AnnotType_key = vat._AnnotType_key
			and va._Term_key = dc._DescendentObject_key
			and va._Term_key = t._Term_key
			and t.isObsolete = 0
			and t._Vocab_key = %d
			and va._Object_key = m._Marker_key
			and va._Qualifier_key not in (1614151, 1614153, 1614155)
			and m._Organism_key = 1
			and vat._MGIType_key = 2''' % vocabKey,

		# 2. mouse markers with alleles which are directly annotated
		# to OMIM terms (no restriction on qualifier)
		'''select va._Term_key,
			aa._Marker_key
		from voc_annot va,
			all_allele aa,
			voc_term t
		where va._AnnotType_key = 1012
			and va._Term_key = t._Term_key
			and t._Vocab_key = %d
			and aa._Allele_key = va._Object_key''' % vocabKey,

		# 3. markers for alleles involved in genotypes which are
		# annotated to the terms themselves
		# (all annotation types which go to genotypes)
		'''select distinct va._Term_key,
			aa._Marker_key
		from voc_annot va,
			voc_annottype vat,
			all_allele aa,
			gxd_allelegenotype gag,
			voc_term t
		where va._AnnotType_key = vat._AnnotType_key
			and va._Term_key = t._Term_key
			and t._Vocab_key = %d
			and t.isObsolete = 0
			and va._Object_key = gag._Genotype_key
			and gag._Allele_key = aa._Allele_key
			and va._Qualifier_key not in (2181424, 1614157)
			and vat._MGIType_key = 12''' % vocabKey,

		# 4. markers for alleles involved in genotypes which are
		# annotated to the terms' descendents
		# (all annotation types which go to genotypes)
		'''select distinct dc._AncestorObject_key as _Term_key,
			aa._Marker_key
		from voc_annot va,
			dag_closure dc,
			voc_annottype vat,
			all_allele aa,
			gxd_allelegenotype gag,
			voc_term t
		where va._AnnotType_key = vat._AnnotType_key
			and va._Term_key = dc._DescendentObject_key
			and va._Term_key = t._Term_key
			and t._Vocab_key = %d
			and t.isObsolete = 0
			and va._Object_key = gag._Genotype_key
			and gag._Allele_key = aa._Allele_key
			and va._Qualifier_key not in (2181424, 1614157)
			and vat._MGIType_key = 12''' % vocabKey,

		# 5. markers are associated with Protein Ontology terms as
		# marker accession IDs, rather than through the annotation
		# tables.  So, pick them up separately.
		'''select ta._Object_key as _Term_key,
			ma._Object_key as _Marker_key
		from acc_accession ma, acc_accession ta, voc_term t
		where ma._MGIType_key = 2
			and ma._LogicalDB_key = 135
			and ma.private = 0
			and ma.accID = ta.accID
			and ta._Object_key = t._Term_key
			and t._Vocab_key = %d
			and ta._MGIType_key = 13''' % vocabKey,
	]

	# queries 0-5 to collect marker/term relationships
	for cmd in cmds:
		(cols, rows) = dbAgnostic.execute (cmd)

		termCol = dbAgnostic.columnNumber (cols, '_Term_key')
		markerCol = dbAgnostic.columnNumber (cols, '_Marker_key')

		for row in rows:
			if not markersPerTerm.has_key(row[termCol]):
				markersPerTerm[row[termCol]] = {}

			markersPerTerm[row[termCol]][row[markerCol]] = 1

	logger.debug ('Found marker counts for %d terms' % len(markersPerTerm))

	# get all terms in this vocab

	cmd = '''select _Term_key
		from voc_term
		where _Vocab_key = %d''' % vocabKey

	cols, rows = dbAgnostic.execute (cmd)

	for row in rows:
		termsInCurrentVocab[row[0]] = 1

	gc.collect()
	logger.debug ('Found %d terms in vocab' % len(termsInCurrentVocab))
	return

def filterBy (fullset, subset):
	c = {}
	for key in fullset.keys():
		if subset.has_key(key):
			c[key] = 1 
	return c

###--- Functions ---###

def getMarkerCount (termKey):
	# get the count of markers associated to the given term

	_initializeVocab(termKey)

	if markersPerTerm.has_key(termKey):
		return len(markersPerTerm[termKey])
	return 0

def getLitIndexMarkerCount (termKey):
	# get the count of markers associated to the given term, where those
	# markers are in the GXD Literature Index

	_initializeMarkerSets()
	_initializeVocab(termKey)

	if markersPerTerm.has_key(termKey):
		return len(filterBy(markersPerTerm[termKey],
			markersInGxdIndex))
	return 0

def getExpressionMarkerCount (termKey):
	# get the count of markers associated to the given term, where those
	# markers also have expression data

	_initializeMarkerSets()
	_initializeVocab(termKey)

	if markersPerTerm.has_key(termKey):
		return len(filterBy(markersPerTerm[termKey],
			markersWithExpressionData))
	return 0

def getCreMarkerCount (termKey):
	# get the count of markers associated to the given term, where those
	# markers also have cre expression data

	_initializeMarkerSets()
	_initializeVocab(termKey)

	if markersPerTerm.has_key(termKey):
		return len(filterBy(markersPerTerm[termKey],
			markersWithCreData))
	return 0

def markerHasCre (markerKey):

	_initializeMarkerSets()

	return markerKey in markersWithCreData

def reset():
	# resets this module to free up memory used

	global initializedMarkerSets, markersWithCreData, markersInGxdIndex
	global markersWithExpressionData, currentVocabKey, markersPerTerm
	global termsInCurrentVocab

	initializedMarkerSets = False
	currentVocabKey = None

	markersWithCreData = {}
	markersWithExpressionData = {}
	markersInGxdIndex = {}
	markersPerTerm = {}
	termsInCurrentVocab = {}

	gc.collect()
	return
