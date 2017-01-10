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

HOMOLOGENE = 9272151
HYBRID = 13764519
HGNC = 13437099
HOMOLOGY = 9272150
DODAG = 50

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

MP = 5		# vocab key for 'Mammalian Phenotype'
PRO = 77	# vocab key for 'Protein Ontology'
DO = 125	# vocab key for 'DO'

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

def _proteinOntologyQueries(vocabKey):
	cmds = [
		# markers are associated with Protein Ontology terms as
		# marker accession IDs, rather than through the annotation
		# tables.  So, pick them up via acc_accession.
		'''select ta._Object_key as _Term_key,
			ma._Object_key as _Marker_key
		from acc_accession ma, acc_accession ta, voc_term t
		where ma._MGIType_key = 2
			and ma._LogicalDB_key = 135
			and ma.private = 0
			and ma.accID = ta.accID
			and ta._Object_key = t._Term_key
			and t._Vocab_key = %d
			and ta._MGIType_key = 13''' % vocabKey
		]
	return cmds 

def _vocabDefaultQueries(vocabKey):
	# fall back on these queries, if this vocab doesn't have its own
	# customized set of queries.

	cmds = [
		# mouse markers annotated to the terms themselves
		# (all annotation types which are direct to markers).
		# skip: four GO top-level terms
		# skip: annotations with NOT qualifiers
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

		# mouse markers annotated to the terms' descendents
		# (all annotation types which are direct to markers)
		# skip: annotations with NOT qualifiers
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
		]
	return cmds

def _extraDoQueries(vocabKey):
	cmds = [
		# mouse markers with alleles which are directly annotated
		# to DO terms (no restriction on qualifier)
## US25 : We no longer include markers in the count due to directly annotated
## alleles. (Oct 2014)
##		'''select distinct va._Term_key,
##			aa._Marker_key
##		from voc_annot va,
##			all_allele aa,
##			voc_term t
##		where va._AnnotType_key = 1012
##			and va._Term_key = t._Term_key
##			and t._Vocab_key = %d
##			and aa._Allele_key = va._Object_key''' % vocabKey,

		# mouse markers which are associated with human markers via
		# a homology relationship, where those human markers are
		# associated with DO diseases
## US45 - 46 we need the DO terms to go down the dag
#		'''
#      select m._Marker_key,
#  			a._Term_key
#  		from voc_annot a,
#  			voc_term q,
#  			mrk_marker h,
#  			mrk_clustermember hcm,
#  			mrk_cluster mc,
#  			mrk_clustermember mcm,
#  			mrk_marker m
#  		where a._AnnotType_key = 1022
#  			and a._Qualifier_key = q._Term_key
#  			and q.term is null
#  			and a._Object_key = h._Marker_key
#  			and h._Organism_key = 2
#  			and h._Marker_key = hcm._Marker_key
#  			and hcm._Cluster_key = mc._Cluster_key
#  			and mc._ClusterType_key = %d
#			and mc._ClusterSource_key in (%d)
#  			and mc._Cluster_key = mcm._Cluster_key
#  			and mcm._Marker_key = m._Marker_key
#  			and m._Organism_key = 1
#  			and m._Marker_Status_key = 1''' % (HOMOLOGY,
#				HYBRID),
     '''
     select m._Marker_key,
         dc._ancestorobject_key as _Term_key
      from mgd.voc_annot a,
         mgd.voc_term q,
         mgd.mrk_marker h,
         mgd.mrk_clustermember hcm,
         mgd.mrk_cluster mc,
         mgd.mrk_clustermember mcm,
         mgd.mrk_marker m,
         mgd.dag_closure dc
      where a._AnnotType_key = 1022
         and a._Qualifier_key = q._Term_key
         and q.term is null
         and a._Object_key = h._Marker_key
         and h._Organism_key = 2
         and h._Marker_key = hcm._Marker_key
         and hcm._Cluster_key = mc._Cluster_key
         and mc._ClusterType_key = %d
         and mc._ClusterSource_key = %d
         and mc._Cluster_key = mcm._Cluster_key
         and mcm._Marker_key = m._Marker_key
         and m._Organism_key = 1
         and m._Marker_Status_key = 1
         and a._Term_key = dc._descendentobject_key
         and dc._dag_key = %d
    union
      select m._Marker_key,
         a._Term_key
      from mgd.voc_annot a,
         mgd.voc_term q,
         mgd.mrk_marker h,
         mgd.mrk_clustermember hcm,
         mgd.mrk_cluster mc,
         mgd.mrk_clustermember mcm,
         mgd.mrk_marker m
      where a._AnnotType_key = 1022
         and a._Qualifier_key = q._Term_key
         and q.term is null
         and a._Object_key = h._Marker_key
         and h._Organism_key = 2
         and h._Marker_key = hcm._Marker_key
         and hcm._Cluster_key = mc._Cluster_key
         and mc._ClusterType_key = %d
         and mc._ClusterSource_key = %d
         and mc._Cluster_key = mcm._Cluster_key
         and mcm._Marker_key = m._Marker_key
         and m._Organism_key = 1
         and m._Marker_Status_key = 1''' % (HOMOLOGY, HYBRID, DODAG, HOMOLOGY, HYBRID),
		]
	return cmds 

def _rollupQueries(vocabKey):
	# queries to extract marker/term relationships that were computed
	# using rollup rules (in the rollupload product) and stored in the
	# database.  Currently valid for MP and DO.

	cmds = [
		# annotations rolled up to the terms themselves
		# skip: NOT qualifiers, Normal qualifiers, obsolete terms
		'''select distinct va._Object_key as _Marker_key,
			t._Term_key
		from voc_annottype vat,
			voc_annot va,
			voc_term t,
			voc_term q,
			mrk_marker m
		where vat._Vocab_key = %d
			and vat._MGIType_key = 2
			and vat._AnnotType_key = va._AnnotType_key
			and va._Qualifier_key = q._Term_key
			and va._Term_key = t._Term_key
			and t.isObsolete = 0
			and va._Object_key = m._Marker_key
			and m._Organism_key = 1
			and q.term is null''' % vocabKey,

		# ancestor terms for those annotations which were rolled up,
		# used to power searching 'down the DAG'
		# skip: NOT qualifiers, Normal qualifiers, obsolete terms
		'''select distinct va._Object_key as _Marker_key,
			t._Term_key
		from voc_annottype vat,
			voc_annot va,
			dag_closure dc,
			voc_term t,
			voc_term q,
			mrk_marker m
		where vat._Vocab_key = %d
			and vat._MGIType_key = 2
			and vat._AnnotType_key = va._AnnotType_key
			and va._Qualifier_key = q._Term_key
			and va._Term_key = dc._DescendentObject_key
			and dc._AncestorObject_key = t._Term_key
			and t.isObsolete = 0
			and va._Object_key = m._Marker_key
			and m._Organism_key = 1
			and q.term is null''' % vocabKey,
		]
	return cmds

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

	# Wipe the previously cached data (if any), and reclaim its memory
	# for re-use.

	markersPerTerm = {}
	termsInCurrentVocab = {}

	gc.collect()

	# Due to the nature of the DAG, it could be problematic to compute
	# the counts in SQL.  While possible, it would involve heavy use of
	# temp tables.  So, we're going to go the simpler route and just
	# collate the markers for each term and its descendents in code.

	# We have different rules for associating markers with terms,
	# depending on the vocabulary:

	if vocabKey == MP:
		cmds = _rollupQueries(vocabKey)

	elif vocabKey == DO:
		cmds = _rollupQueries(vocabKey) + _extraDoQueries(vocabKey)

	elif vocabKey == PRO:
		cmds = _proteinOntologyQueries(vocabKey)

	else:
		cmds = _vocabDefaultQueries(vocabKey)

	# execute queries and collect marker/term relationships
	for cmd in cmds:
		(cols, rows) = dbAgnostic.execute (cmd)

		termCol = dbAgnostic.columnNumber (cols, '_Term_key')
		markerCol = dbAgnostic.columnNumber (cols, '_Marker_key')

		for row in rows:
			if not markersPerTerm.has_key(row[termCol]):
				markersPerTerm[row[termCol]] = {}

			markersPerTerm[row[termCol]][row[markerCol]] = 1

		cols = []
		rows = []
		gc.collect()

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
