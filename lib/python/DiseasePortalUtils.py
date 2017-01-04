# Module: DiseasePortalUtils.py
# Purpose: to provide utility functions for gatherers interacting with HMDC
#	(aka HDP, aka Disease Portal) data

import dbAgnostic
import logger
import gc

###---------------###
###--- Globals ---###
###---------------###

MP_GENOTYPE = 1002		# annot type for MP-genotype annotations
DO_GENOTYPE = 1020        # annot type for disease-genotype annotations
DO_ALLELE = 1021        # annot type for disease-allele annotations
MP_MARKER = 1015		# annot type for rolled-up MP annotations
DO_MARKER = 1023		# annot type for rolled-up disease annot.
NOT_QUALIFIER = 1614157		# term key for NOT qualifier for disease annot.
VOCAB = 13			# MGI type for vocabulary terms
HYBRID = 13764519		# cluster source for hybrid homology
HOMOLOGY = 9272150		# homology cluster type for MRK_Cluster
DO_HUMAN_MARKER = 1022	# annot type for disease/human marker


###-------------------------###
###--- utility functions ---###
###-------------------------###

indexCt = 0

def nextIndex():
	# get a unique name to use when creating a new index on a temp table

	global indexCt

	indexCt = indexCt + 1
	return 'idx_dpa_%d' % indexCt

###------------------###
###--- MP Headers ---###
###------------------###

mpHeaders = 'mpHeaders'		# section name
mpHeadersLoaded = False		# have we cached the MP headers yet?
mpTermKeyToHeaders = {}		# MP term key : [ MP header terms ]
mpHeaderTermToKey = {}		# MP header term : MP header key

def isMPHeader (headerTerm):
	# returns True if passed a recognized MP header term, False if not

	key = getMPHeaderKey(headerTerm)
	return (key != None)

def getMPHeaderKey (headerTerm):
	# returns the _Term_key for the given header term, or None if it's
	# not a known header term

	if not mpHeadersLoaded:
		_initMPHeaders()

	if mpHeaderTermToKey.has_key(headerTerm):
		return mpHeaderTermToKey[headerTerm]
	return None

def hasMPHeader (mpTermKey):
	# returns True if the given mpTermKey has at least one associated MP
	# header term, False if not.

	if len(getMPHeaders(mpTermKey)) == 0:
		return False
	return True

def getMPHeaders (mpTermKey):
	# returns a list of header terms for the given MP term key, or an
	# empty list of the given key has no known headers

	if not mpHeadersLoaded:
		_initMPHeaders()

	if mpTermKeyToHeaders.has_key(mpTermKey):
		return mpTermKeyToHeaders[mpTermKey]
	return []

def _initMPHeaders():
	# bring in the MP term / header data and cache it in memory

	global mpTermKeyToHeaders, mpHeaderTermToKey, mpHeadersLoaded

	if mpHeadersLoaded:
		return

	# top of the union map MP terms to their headers, while the bottom is
	# for the header terms themselves

	cmd = '''select distinct d._Object_key as term_key,
			h.synonym,
			h._object_key as header_key
		from DAG_Node d, DAG_Closure dc, DAG_Node dh, MGI_Synonym h
		where d._DAG_key = 4
			and d._Node_key = dc._Descendent_key
			and dc._Ancestor_key = dh._Node_key
			and dh._Label_key = 3
			and dh._Object_key = h._object_key
			and h._synonymtype_key = 1021
		union
		select distinct d._Object_key,
			h.synonym,
			h._object_key as header_key
		from DAG_Node d, DAG_Closure dc, DAG_Node dh, MGI_Synonym h
		where d._DAG_key = 4
			and d._Node_key = dc._Descendent_key
			and dc._Descendent_key = dh._Node_key
			and dh._Label_key = 3
			and dh._Object_key = h._object_key
			and h._synonymtype_key = 1021'''

	(cols, rows) = dbAgnostic.execute(cmd)

	termKeyCol = dbAgnostic.columnNumber (cols, 'term_key')
	headerKeyCol = dbAgnostic.columnNumber (cols, 'header_key')
	headerCol = dbAgnostic.columnNumber (cols, 'synonym')
	
	for row in rows:
		termKey = row[termKeyCol]
		headerKey = row[headerKeyCol]
		header = row[headerCol]

		if not mpHeaderTermToKey.has_key(header):
			mpHeaderTermToKey[header] = headerKey

		# map term_keys to their headers
		mpTermKeyToHeaders.setdefault(termKey,[]).append(header)

	del rows, cols
	gc.collect()

	mpHeadersLoaded = True
	logger.debug('Cached %d MP headers' % len(mpHeaderTermToKey))
	logger.debug('Cached headers for %d MP terms' % len(mpTermKeyToHeaders))
	return

def _unloadMPHeaders():
	# reset the global variables that store MP term / header data and
	# free up the memory

	global mpTermKeyToHeaders, mpHeaderTermToKey, mpHeadersLoaded

	mpTermKeyToHeaders = {}
	mpHeaderTermToKey = {}
	mpHeadersLoaded = False
	gc.collect()
	logger.debug('unloaded MP data from DiseasePortalUtils')
	return

###-----------------------###
###--- Disease Headers ---###
###-----------------------###

diseaseHeaders = 'diseaseHeaders'	# section name
diseaseHeadersLoaded = False		# have we cached disease headers yet?
diseaseTermKeyToHeaders = {}		# disease term key : [ disease headers ]

def getDiseaseHeaders (diseaseTermKey):
	# returns a list of header terms for the given disease term key, or an
	# empty list of the given key has no known headers

	if not diseaseHeadersLoaded:
		_initDiseaseHeaders()

	if diseaseTermKeyToHeaders.has_key(diseaseTermKey):
		return diseaseTermKeyToHeaders[diseaseTermKey]
	return []

def hasDiseaseHeader (diseaseTermKey):
	# returns True if the given diseaseTermKey has at least one associated
	# disease header term, False if not.

	if len(getDiseaseHeaders(diseaseTermKey)) == 0:
		return False
	return True

def _initDiseaseHeaders():
	# bring in the disease term / header data and cache it in memory

	global diseaseHeadersLoaded, diseaseTermKeyToHeaders

	if diseaseHeadersLoaded:
		return

	cmd = '''select distinct dc._descendent_key, vt.term
		from DAG_Closure dc, DAG_Node dn, VOC_Term vt, MGI_SetMember sm
		where dc._ancestor_key = dn._Node_key
		and dn._Object_key = vt._term_key
		and dn._Object_key = sm._Object_key
		and sm._Set_key=1048 '''

	(cols, rows) = dbAgnostic.execute(cmd)
	termKey = dbAgnostic.columnNumber (cols, '_descendent_key')
	header = dbAgnostic.columnNumber (cols, 'term')
	
	for row in rows:
		# map term_keys to their headers
		diseaseTermKeyToHeaders.setdefault(row[termKey],[]).append(row[header])
	diseaseHeadersLoaded = True
	return

def _unloadDiseaseHeaders():
	# reset the global variables that store disease term / header data and
	# free up the memory

	global diseaseHeadersLoaded, diseaseTermKeyToHeaders

	diseaseTermKeyToHeaders = {}
	diseaseHeadersLoaded = False
	gc.collect()
	logger.debug('unloaded disease data from DiseasePortalUtils')
	return


###-------------------------------------###
###--- source_annotations temp table ---###
###-------------------------------------###

# In order to trace a rolled-up annotation back to its original genotype, we'll
# need to use the source annotation key stored as a property for each rolled-up
# annotation.  These are varchars and need to be pulled into a temp table as
# integers and indexed.

sourceAnnotationTable = None

def getSourceAnnotationsTable():
	# (builds, if necessary, and) returns the name of a source annotation
	# table with four columns: _AnnotType_key, _Marker_key,
	# _DerivedAnnot_key, and _SourceAnnot_key

	global sourceAnnotationTable

	if not sourceAnnotationTable:
		# build a temp table mapping from each rolled-up annotation to
		# its source annotation
		
		sourceAnnotationTable = 'source_annotations'

		cmd1 = '''select va._AnnotType_key,
				va._Object_key as _Marker_key,
				va._Annot_key as _DerivedAnnot_key,
				p.value::int as _SourceAnnot_key
			into temporary table %s
			from VOC_Annot va,
				VOC_Evidence ve,
				VOC_Evidence_Property p,
				VOC_Term t
			where va._AnnotType_key in (%s, %s)
				and va._Annot_key = ve._Annot_key
				and ve._AnnotEvidence_key = p._AnnotEvidence_key
				and p._PropertyTerm_key = t._Term_key
				and va._Qualifier_key != %s
				and t.term = '_SourceAnnot_key' ''' % (
					sourceAnnotationTable,
					MP_MARKER, DO_MARKER, NOT_QUALIFIER)

		dbAgnostic.execute(cmd1)
		logger.debug('Built temp table %s' % sourceAnnotationTable)

		fieldsToIndex = [ '_DerivedAnnot_key', '_SourceAnnot_key',
			'_Marker_key', '_AnnotType_key' ]

		cmd2 = 'create index %s on %s (%s)'

		for field in fieldsToIndex:
			dbAgnostic.execute(cmd2 % (nextIndex(),
				sourceAnnotationTable, field))
		logger.debug('Indexed temp table %s' % sourceAnnotationTable)

	return sourceAnnotationTable


###-------------------###
###--- annotations ---###
###-------------------###

def getAnnotations(filterClause = ''):
	# returns a (cols, rows) tuple which contains the pre-computed MP and
	# disease annotations.  See query for which columns are returned.
	# 'filterClause' can be used to specify a clause to add to the WHERE
	# part of the query, if we want to further restrict the result set.

	if filterClause:
		filterClause = ' and %s' % filterClause

	cmd = '''select distinct m._Marker_key,
			m._Organism_key,
			src._AnnotType_key,
			src._Term_key,
			src._Object_key as _Genotype_key,
			a.accID,
			t.term,
			v.name,
			q.term as qualifier_type,
			src._Qualifier_key,
			null as genotype_type
		from VOC_Annot va,
			MRK_Marker m,
			%s s,
			VOC_Annot src,
			VOC_Term t,
			ACC_Accession a,
			VOC_Term q,
			VOC_Vocab v
		where va._AnnotType_key in (%d, %d)
			and va._Object_key = m._Marker_key
			and va._Annot_key = s._DerivedAnnot_key
			and s._SourceAnnot_key = src._Annot_key
			and src._Term_key = t._Term_key
			and src._Term_key = a._Object_key
			and a._MGIType_key = %d
			and a.private = 0
			and a.preferred = 1
			and src._Qualifier_key = q._Term_key
			and src._Qualifier_key != %s
			and t._Vocab_key = v._Vocab_key
			%s''' % (
				getSourceAnnotationsTable(),
				DO_MARKER, MP_MARKER, VOCAB, NOT_QUALIFIER,
				filterClause)

	cols, rows = dbAgnostic.execute(cmd)

	if filterClause:
		logger.debug('Got %d annot rows (%s)' % (len(rows),
			filterClause))
	else:
		logger.debug('Got %d annot rows' % len(rows))

	return cols, rows

def getReferencesByDiseaseKey():
	# get a dictionary mapping from an OMIM disease term key to a list of 
	# reference keys, excluding annotations with NOT qualifiers.  For
	# disease/allele annotations, we only include the references if the
	# term is also associated with a genocluster.

	# collect a dictionary of term keys that were rolled up to markers

	annotCols, annotRows = getAnnotations('s._AnnotType_key = %d' % \
		DO_MARKER)

	termCol = dbAgnostic.columnNumber(annotCols, '_Term_key')

	rolledUpDiseases = {}
	for row in annotRows:
		rolledUpDiseases[row[termCol]] = 1

	del annotCols, annotRows
	gc.collect()

	logger.debug('Got %d diseases that rolled up' % len(rolledUpDiseases))

	# now collect a dictionary that maps from each rolled-up term to its
	# references

	# distinct set of references for positive annotations from a
	# genotype to a disease term and from an allele directly to a disease
	# term
	cmd = '''select distinct v._Term_key, e._Refs_key
		from VOC_Annot v, VOC_Evidence e
		where (
		    (v._AnnotType_key = %d and v._Qualifier_key != %d)
		    or v._AnnotType_key = %d
		    )
		and v._Annot_key = e._Annot_key''' % (
			DO_GENOTYPE, NOT_QUALIFIER, DO_ALLELE)

	cols, rows = dbAgnostic.execute(cmd)

	termCol = dbAgnostic.columnNumber(cols, '_Term_key')
	refsCol = dbAgnostic.columnNumber(cols, '_Refs_key')

	termToRefs = {}			# term key -> [ refs key 1, ... ]
	for row in rows:
		term = row[termCol]

		# only include the reference if the term also survived the
		# rollup rules

		if rolledUpDiseases.has_key(term):
			if termToRefs.has_key(term):
				termToRefs[term].append(row[refsCol])
			else:
				termToRefs[term] = [ row[refsCol] ]

	del cols, rows
	gc.collect()

	logger.debug('Got refs for %d diseases' % len(termToRefs))
	return termToRefs

###---------------------###
###--- grid clusters ---###
###---------------------###

gridClusters = 'gridClusters'		# section name
gridClustersLoaded = False		# has the grid cluster data been cached?

# { marker key : [ symbol, organism, homology cluster key, homologene ID ] }
gridClusterMarkers = {}	

# table of term/marker pairs where markers are in a homology cluster
rollupWithClustersTable = None

# table of term/marker pairs where markers are not in a homology cluster
rollupNoClustersTable = None 

def getRollupWithClustersTable():
	# get the name of a temp table with marker/term associations (for MP
	# and disease annotations), where the markers are part of a homology
	# cluster.  Fields in the temp table include _Cluster_key,
	# _Marker_key, _AnnotType_key, _Term_key, term, and accID.

	global rollupWithClustersTable

	if rollupWithClustersTable:
		return rollupWithClustersTable

	rollupWithClustersTable = 'tmp_cluster'

	# top of the union brings in rolled-up MP and disease annotations for
	# mouse markers which are in homology clusters.  Bottom of the union
	# brings in disease annotations to human markers and human phenotypic
	# markers which are part of homology clusters.

	cmd = '''select distinct c._Cluster_key,
			s._Object_key as _Marker_key,
			s._AnnotType_key, s._Term_key, t.term, a.accID
		into temporary table %s
		from VOC_Annot s, MRK_ClusterMember c, MRK_Cluster mc,
			VOC_Term t, ACC_Accession a
		where s._Object_key = c._Marker_key
			and s._AnnotType_key in (%d, %d)
			and c._Cluster_key = mc._Cluster_key
			and mc._ClusterSource_key = %d
			and mc._ClusterType_key = %d
			and s._Term_key = t._Term_key
			and s._Term_key = a._Object_key
			and a._MGIType_key = %d
			and a.preferred = 1
			and s._Qualifier_key != %d
		union
		select distinct c._Cluster_key, c._Marker_key,
			v._AnnotType_key, v._Term_key, t.term, a.accID
		from MRK_ClusterMember c, VOC_Annot v, MRK_Cluster mc,
			VOC_Term t, ACC_Accession a
		where c._Marker_key = v._Object_key
			and v._AnnotType_key in (%d)
			and c._Cluster_key = mc._Cluster_key
			and mc._ClusterSource_key = %d
			and mc._ClusterType_key = %d
			and v._Term_key = t._Term_key
			and v._Term_key = a._Object_key
			and a._MGIType_key = %d
			and v._Qualifier_key != %d
			and a.preferred = 1''' % (rollupWithClustersTable,
				MP_MARKER, DO_MARKER, HYBRID, HOMOLOGY,
				VOCAB, NOT_QUALIFIER, DO_HUMAN_MARKER,
				HYBRID, HOMOLOGY,
				VOCAB, NOT_QUALIFIER)

	dbAgnostic.execute(cmd)
	logger.debug('populated %s' % rollupWithClustersTable)

	dbAgnostic.execute('create index %s on %s (_Cluster_key)' % (
		nextIndex(), rollupWithClustersTable))

	dbAgnostic.execute('create index %s on %s (_Marker_key)' % (
		nextIndex(), rollupWithClustersTable))

	logger.debug('indexed %s' % rollupWithClustersTable)
	return rollupWithClustersTable

def getRollupNoClustersTable():
	# get the name of a temp table with marker/term associations (for MP
	# and disease annotations), where the markers are not part of a
	# homology cluster.  Fields in the temp table include _Marker_key,
	# _AnnotType_key, _Term_key, term, and accID.

	global rollupNoClustersTable

	if rollupNoClustersTable:
		return rollupNoClustersTable

	rollupNoClustersTable = 'tmp_nocluster'

	# top of the union brings in rolled-up MP and disease annotations for
	# mouse markers which are not in homology clusters.  Bottom of the
	# union brings in disease annotations to human markers and human
	# phenotypic markers which are not part of homology clusters.

	cmd = '''select distinct s._Object_key as _Marker_key,
			s._AnnotType_key, s._Term_key, t.term, a.accID
		into temporary table %s
		from VOC_Annot s, VOC_Term t, ACC_Accession a
		where not exists (select 1 from %s tc
				where s._Object_key = tc._Marker_key)
			and s._AnnotType_key in (%d, %d)
			and s._Qualifier_key != %d
			and s._Term_key = t._Term_key
			and s._Term_key = a._Object_key
			and a._MGIType_key = %d
			and a.preferred = 1
		union
		select distinct c._Marker_key, v._AnnotType_key, v._Term_key,
			t.term, a.accID
		from MRK_Marker c, VOC_Annot v, VOC_Term t, ACC_Accession a
		where c._Marker_key = v._Object_key
			and v._AnnotType_key in (%d)
			and v._Term_key = t._Term_key
			and v._Term_key = a._Object_key
			and a._MGIType_key = %d
			and v._Qualifier_key != %d
			and a.preferred = 1
			and not exists (select 1 from %s tc
				where c._Marker_key = tc._Marker_key)''' % (
			rollupNoClustersTable, getRollupWithClustersTable(),
			MP_MARKER, DO_MARKER, NOT_QUALIFIER, VOCAB,
			DO_HUMAN_MARKER, VOCAB,
			NOT_QUALIFIER, getRollupWithClustersTable())

	dbAgnostic.execute(cmd)
	logger.debug('populated %s' % rollupNoClustersTable)

	dbAgnostic.execute('create index %s on %s (_Marker_key)' % (
		nextIndex(), rollupNoClustersTable))
	logger.debug('indexed %s' % rollupNoClustersTable)

	return rollupNoClustersTable

def getClusteredMarkers():
	# get a set of data for markers with MP and/or disease annotations,
	# where those markers are in homology clusters.  Returns a tuple of
	# (cols, rows), where the columns include:  _Cluster_key, _Marker_key,
	# and homologene_id (optional).  Refer to MarkerUtils.py if you need
	# other marker attributes.

	cmd = '''select distinct c._Cluster_key, c._Marker_key,
			a.accID as homologene_id
		from MRK_Cluster mc
		inner join MRK_ClusterMember c on (
			c._Cluster_key = mc._Cluster_key)
		inner join MRK_Marker m on (c._Marker_key = m._Marker_key
			and m._Organism_key in (1,2))
		left outer join ACC_Accession a on (
			mc._Cluster_key = a._Object_key
			and a._LogicalDB_key = 81)
		where mc._ClusterSource_key = %d
			and mc._ClusterType_key = %d
			and exists (select 1 from %s tc 
				where tc._Cluster_key = mc._Cluster_key)
		order by c._Cluster_key''' % (HYBRID, HOMOLOGY,
			getRollupWithClustersTable())

	cols, rows = dbAgnostic.execute(cmd)
	logger.debug('Got %d rows of clustered markers' % len(rows))
	return cols, rows

def getNonClusteredMarkers():
	# get a set of data for markers with MP and/or disease annotations,
	# where those markers are not in homology clusters.  Returns a tuple of
	# (cols, rows), where the column returned is _Marker_key.  Refer to
	# MarkerUtils.py if you need other marker attributes.

 	cmd = '''select distinct c._Marker_key
		from MRK_Marker c
		where c._Organism_key in (1,2)
			and exists (select 1 from %s tc
				where tc._Marker_key = c._Marker_key)
		order by c._Marker_key''' % getRollupNoClustersTable()

	cols, rows = dbAgnostic.execute(cmd)
	logger.debug('Got %d rows of non-clustered markers' % len(rows))
	return cols, rows

def _initGridClusters():
	# pre-load any needed global variables for the grid clusters data

	global gridClusterMarkers, gridClustersLoaded

	if gridClustersLoaded:
		return
	return

def _unloadGridClusters():
	# remove any cached data related to grid clusters, so the memory can
	# be freed up

	global gridClustersLoaded, gridClusterMarkers

	gridClustersLoaded = False
	gridClusterMarkers = {}	
	return

###-----------------------------###
###--- Module-wide functions ---###
###-----------------------------###

def unload(section = 'all'):
	# remove the cached data from memory, either for a single section (if
	# specified) or from all of them if not

	if section in [ 'all', mpHeaders ]:
		_unloadMPHeaders()
	if section in [ 'all', diseaseHeaders ]:
		_unloadDiseaseHeaders()
	if section in [ 'all', gridClusters ]:
		_unloadGridClusters()
	gc.collect()
	return
