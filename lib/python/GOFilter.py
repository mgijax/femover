# Module: GOFilter.py
# Purpose: to provide an easy means to determine which GO annotations should
#	be included (and counted) in the front-end database, and which should
#	be omitted.  The basic rules are:
#	1. All annotations should be included if they have an evidence code
#		other than ND (No Data).
#	2. Annotations with an ND evidence code should only be included if
#		there are no other annotations to that GO DAG (process,
#		function, component) for that marker.

import dbAgnostic
import logger

###--- Globals ---###

# has this module been initialized?
INITIALIZED = False

# maps from marker key to dictionary with DAG keys found so far.
#	MARKER_DAGS[marker key] = { dag key : 1 }
MARKER_DAGS = {}

# stores annotation keys which we want to keep in the front-end database
#	KEEPER_ANNOTATIONS[annot key] = 1
KEEPER_ANNOTATIONS = {}

# stores tuples which define the annotations kept so far for each marker
# 	PER_MARKER[marker key] = { (annotation tuple) : 1 }
PER_MARKER = {}

###--- Private Functions ---###

def _loadAnnotationsNoND():
	# load global variables with data from all GO annotations except those
	# with a ND (No data) evidence code

	global MARKER_DAGS, KEEPER_ANNOTATIONS, PER_MARKER

	# all GO annotations where evidence term is not ND
	cmd1 = '''select distinct a._Object_key as _Marker_key,
			a._Term_key, n._DAG_key, a._Annot_key,
			a._Qualifier_key, e.inferredFrom, e._EvidenceTerm_key
		from voc_annot a, voc_evidence e, dag_node n
		where a._AnnotType_key = 1000
			and a._Annot_key = e._Annot_key
			and a._Term_key = n._Object_key
			and n._DAG_key in (1,2,3)
			and e._EvidenceTerm_key != 118'''
	
	(cols, rows) = dbAgnostic.execute (cmd1)

	markerCol = dbAgnostic.columnNumber (cols, '_Marker_key')
	termCol = dbAgnostic.columnNumber (cols, '_Term_key')
	dagCol = dbAgnostic.columnNumber (cols, '_DAG_key')
	annotCol = dbAgnostic.columnNumber (cols, '_Annot_key')
	qualifierCol = dbAgnostic.columnNumber (cols, '_Qualifier_key')
	inferredCol = dbAgnostic.columnNumber (cols, 'inferredFrom')
	evidenceCol = dbAgnostic.columnNumber (cols, '_EvidenceTerm_key')

	for row in rows:
		markerKey = row[markerCol]
		dagKey = row[dagCol]

		# flag the DAG for this annotation's term for the marker

		if not MARKER_DAGS.has_key(markerKey):
			MARKER_DAGS[markerKey] = { dagKey : 1 }
		else:
			MARKER_DAGS[markerKey][dagKey] = 1

		# we keep all non-ND annotations

		KEEPER_ANNOTATIONS[row[annotCol]] = 1

		# track unique annotations for this marker.  A unique
		# annotation is defind as a unique set of marker key, term,
		# qualifier, evidence code, and inferred-from value.

		annotTuple = (row[termCol], row[qualifierCol],
			row[evidenceCol], row[inferredCol])

		if not PER_MARKER.has_key(markerKey):
			PER_MARKER[markerKey] = { annotTuple : 1 }
		else:
			PER_MARKER[markerKey][annotTuple] = 1

	logger.debug ('Processed %d non-ND GO annotations' % len(rows)) 
	return

def _loadAnnotationsOnlyND():
	# load global variables with data from all GO annotations which have
	# a ND (No data) evidence code

	global MARKER_DAGS, KEEPER_ANNOTATIONS, PER_MARKER

	# all GO annotations where evidence term is ND
	cmd2 = '''select distinct a._Object_key as _Marker_key,
			a._Term_key, n._DAG_key, a._Annot_key,
			a._Qualifier_key, e.inferredFrom, e._EvidenceTerm_key
		from voc_annot a, voc_evidence e, dag_node n
		where a._AnnotType_key = 1000
			and a._Annot_key = e._Annot_key
			and a._Term_key = n._Object_key
			and n._DAG_key in (1,2,3)
			and e._EvidenceTerm_key = 118'''

	(cols, rows) = dbAgnostic.execute (cmd2)

	markerCol = dbAgnostic.columnNumber (cols, '_Marker_key')
	termCol = dbAgnostic.columnNumber (cols, '_Term_key')
	dagCol = dbAgnostic.columnNumber (cols, '_DAG_key')
	annotCol = dbAgnostic.columnNumber (cols, '_Annot_key')
	qualifierCol = dbAgnostic.columnNumber (cols, '_Qualifier_key')
	inferredCol = dbAgnostic.columnNumber (cols, 'inferredFrom')
	evidenceCol = dbAgnostic.columnNumber (cols, '_EvidenceTerm_key')

	kept = 0

	for row in rows:
		markerKey = row[markerCol]
		dagKey = row[dagCol]

		# if this marker already has an annotation for this DAG, then
		# skip this one.  Otherwise, keep it and flag the DAG.

		if MARKER_DAGS.has_key(markerKey):
			if MARKER_DAGS[markerKey].has_key(dagKey):
				continue

			MARKER_DAGS[markerKey][dagKey] = 1
		else:
			MARKER_DAGS[markerKey] = { dagKey : 1 }

		# If we made it to this point, then we are keeping this ND
		# annotation.

		kept = kept + 1
		KEEPER_ANNOTATIONS[row[annotCol]] = 1

		# track unique annotations for this marker.  A unique
		# annotation is defind as a unique set of marker key, term,
		# qualifier, evidence code, and inferred-from value.

		annotTuple = (row[termCol], row[qualifierCol],
			row[evidenceCol], row[inferredCol])

		if not PER_MARKER.has_key(markerKey):
			PER_MARKER[markerKey] = { annotTuple : 1 }
		else:
			PER_MARKER[markerKey][annotTuple] = 1

	logger.debug ('Processed %d ND GO annotations, kept %d' % (len(rows),
		kept) ) 
	return

def _initialize():
	global INITIALIZED

	# Our strategy for determining which annotations to keep is:
	# 1. keep all annotations which do not have an ND evidence code
	# 2. for any annotation with an ND evidence code, only keep it if that
	#	marker has no other annotations for that particular DAG

	_loadAnnotationsNoND()
	_loadAnnotationsOnlyND()

	INITIALIZED = True
	return

###--- Functions ---###

def getAnnotationCount (markerKey):
	# get a count of GO annotations for the given 'markerKey'

	if not INITIALIZED:
		_initialize()

	if PER_MARKER.has_key(markerKey):
		return len(PER_MARKER[markerKey])
	return 0
	
def shouldInclude (annotKey):
	# determine if the annotation with this 'annotKey' should be included
	# in the front-end database (True) or not (False)

	if not INITIALIZED:
		_initialize()

	if KEEPER_ANNOTATIONS.has_key(annotKey):
		return True
	return False

def getMarkerKeys():
	# return the list of marker keys which have GO annotations

	if not INITIALIZED:
		_initialize()

	keys = PER_MARKER.keys()
	keys.sort()

	return keys
