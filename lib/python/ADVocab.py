# Module: ADVocab.py
# Purpose: to read from the GXD anatomical dictionary tables (in mgd), which
#	are outside the typical vocabulary tables, and to provide easy access
#	to bring them into the vocabulary tables in the front-end database
# Assumptions:  Technically, the AD structure is a tree rather than a DAG.
#	That is, each node has only one parent, so there is only a single path
#	to get to each node.
#
# 07/26/2012	lec
#	- TR10269/removed 'conceptus' hack
#

import dbAgnostic
import logger
import types
import symbolsort

###--- Globals ---###

error = 'ADVocab.error'		# exception to be raised in this module

AD_VOCAB_NAME = 'Anatomical Dictionary'		# name for AD vocabulary
EDGE_LABEL = None				# label for AD edges

AD_VOCAB_KEY = None		# vocabulary key to use for AD
AD_TERM_KEY_OFFSET = None	# maximum term key for other vocabularies

MAX_DEPTH = 0			# maximum AD tree depth seen so far

MGI_ID = {}			# structure key -> MGI ID
SYNONYMS = {}			# structure key -> [ synonyms ]
DESCENDENT_COUNT = {}		# structure key -> count of descendents
TERM = {}			# structure key -> { field : value }

ANCESTORS = {}			# structure key -> [ ancestor 1, ... ]
CHILDREN = {}			# structure key -> [ child 1, ... ]
DESCENDENTS = {}		# structure key -> [ descendent 1, ... ]
ROOTS = {}			# root structure key -> 1
LEAVES = {}			# leaf structure key -> 1

STRUCTURE_KEYS = []		# list of all structure keys

PARENT = 'parentKey'		# constants for fields in TERM records
STRUCTURE = 'term'
STAGE = 'theilerStage'
SYSTEM = 'system'
EDINBURGH = 'edinburghKey'
PRINTNAME = 'printname'
DEPTH = 'treeDepth'
SORT = 'topoSort'
TERMKEY = '_Term_key'

###--- Private Functions ---###

def _vocabDagTermSort (a, b):
	# compare two terms (stage, lowercase term, lowercase printname,
	# term key) for sorting

	if a[0] != b[0]:
		return cmp(a[0], b[0])

	if a[1] != b[1]:
		return symbolsort.nomenCompare(a[1], b[1])

	return symbolsort.nomenCompare(a[2], b[2])

def _loadConstants():
	global AD_VOCAB_KEY, AD_TERM_KEY_OFFSET

	if AD_VOCAB_KEY != None:
		return

	# find what vocabulary key we need to use

	query1 = 'select max(_Vocab_key) from VOC_Vocab'

	(cols, rows) = dbAgnostic.execute (query1)

	for row in rows:
		AD_VOCAB_KEY = 1 + row[0]

	logger.debug ('AD is vocab key: %d' % AD_VOCAB_KEY)

	# find what the current maximum term key is, so we have an offset to
	# use for bringing in AD structure keys

	query2 = 'select max(_Term_key) from VOC_Term'

	(cols, rows) = dbAgnostic.execute (query2)

	for row in rows:
		AD_TERM_KEY_OFFSET = row[0]

	logger.debug ('AD term key offset: %d' % AD_TERM_KEY_OFFSET)
	return 

def _loadIDs():
	global MGI_ID

	if len(MGI_ID) > 0:
		return

	# get the primary MGI ID for each structure

	query3 = '''select _Object_key, accID
		from ACC_Accession
		where _MGIType_key = 38
			and preferred = 1
			and _LogicalDB_key = 1'''

	(cols, rows) = dbAgnostic.execute (query3)

	keyCol = dbAgnostic.columnNumber (cols, '_Object_key')
	idCol = dbAgnostic.columnNumber (cols, 'accID')

	MGI_ID = {}
	for row in rows:
		MGI_ID[row[keyCol]] = row[idCol]

	logger.debug ('Found IDs for %d AD terms' % len(MGI_ID))
	return

def _loadSynonyms():
	global SYNONYMS

	if len(SYNONYMS) > 0:
		return

	# get the synonyms for each structure

	query4 = '''select s._Structure_key,
			n.structure
		from GXD_Structure s,
			GXD_StructureName n
		where s._Structure_key = n._Structure_key
			and s._StructureName_key != n._StructureName_key'''

	(cols, rows) = dbAgnostic.execute (query4)

	keyCol = dbAgnostic.columnNumber (cols, '_Structure_key')
	synonymCol = dbAgnostic.columnNumber (cols, 'structure')

	SYNONYMS = {}
	for row in rows:
		key = row[keyCol]
		if SYNONYMS.has_key (key):
			SYNONYMS[key].append (row[synonymCol])
		else:
			SYNONYMS[key] = [ row[synonymCol] ]
	
	for key in SYNONYMS.keys():
		SYNONYMS[key].sort()

	logger.debug ('Found %d synonyms for %d AD terms' % (len(rows),
		len(SYNONYMS)) )
	return

def _loadDescendentCounts():
	global DESCENDENT_COUNT

	if len(DESCENDENT_COUNT) > 0:
		return DESCENDENT_COUNT

	# get the count of descendents for each term

	query5 = '''select _Structure_key, count(1) as myCount
		from GXD_StructureClosure
		group by _Structure_key'''

	(cols, rows) = dbAgnostic.execute (query5)

	keyCol = dbAgnostic.columnNumber (cols, '_Structure_key')
	countCol = dbAgnostic.columnNumber (cols, 'myCount')

	DESCENDENT_COUNT = {}
	for row in rows:
		DESCENDENT_COUNT[row[keyCol]] = row[countCol]

	logger.debug ('Got descendent counts for %d AD terms' % \
		len(DESCENDENT_COUNT))
	return DESCENDENT_COUNT

def _loadTermData():
	global TERM, MAX_DEPTH

	if (len(TERM) > 0) and (MAX_DEPTH > 0):
		return

	_loadConstants()

	# get the basic term data

	query6 = '''select s._Structure_key, 
			s._Parent_key,
			n.structure as term,
			ts.stage as theiler_stage,
			t.term as system,
			s.edinburghKey,
			s.printname,
			s.treeDepth,
			s.topoSort
		from GXD_Structure s,
			GXD_StructureName n,
			GXD_TheilerStage ts,
			VOC_Term t
		where s._Structure_key = n._Structure_key
			and s._StructureName_key = n._StructureName_key
			and s._Stage_key = ts._Stage_key
			and s._System_key = t._Term_key'''

	(cols, rows) = dbAgnostic.execute (query6)

	structureCol = dbAgnostic.columnNumber (cols, '_Structure_key')
	parentCol = dbAgnostic.columnNumber (cols, '_Parent_key')
	termCol = dbAgnostic.columnNumber (cols, 'term')
	stageCol = dbAgnostic.columnNumber (cols, 'theiler_stage')
	systemCol = dbAgnostic.columnNumber (cols, 'system')
	edinburghCol = dbAgnostic.columnNumber (cols, 'edinburghKey')
	printnameCol = dbAgnostic.columnNumber (cols, 'printname')
	depthCol = dbAgnostic.columnNumber (cols, 'treeDepth')
	sortCol = dbAgnostic.columnNumber (cols, 'topoSort')

	TERM = {}
	MAX_DEPTH = 0

	for row in rows:
		key = row[structureCol]

		# convert depth to be 1-based for consistency with other
		# vocabularies

		depth = row[depthCol] + 1
		MAX_DEPTH = max(MAX_DEPTH, depth)

		term = row[termCol]

		printName = row[printnameCol]
		if (printName is None) or (printName.strip() == ''):
			printName = 'conceptus'

		TERM[key] = {
			PARENT    : row[parentCol],
			STRUCTURE : term,
			STAGE     : row[stageCol],
			SYSTEM    : row[systemCol],
			EDINBURGH : row[edinburghCol],
			PRINTNAME : printName,
			DEPTH     : depth,
			SORT      : row[sortCol],
			TERMKEY   : key + AD_TERM_KEY_OFFSET,
			}

	logger.debug ('Got basic data for %d AD terms' % len(TERM))
	return

def _loadChildren():
	global CHILDREN, ROOTS

	if len(CHILDREN) > 0:
		return CHILDREN

	_loadTermData()

	for childKey in _getStructureKeys():

		parentKey = TERM[childKey][PARENT]
		if parentKey is None:			# is a root node
			ROOTS[childKey] = 1

		if CHILDREN.has_key(parentKey):
			CHILDREN[parentKey].append (childKey)
		else:
			CHILDREN[parentKey] = [ childKey ]

	logger.debug ('Got children for %d terms' % len(CHILDREN))
	logger.debug ('Got %d root terms' % len(ROOTS))
	return CHILDREN

def _loadRoots():
	if len(ROOTS) > 0:
		return ROOTS

	_loadChildren()
	return ROOTS

def _loadAncestors():
	global ANCESTORS

	if len(ANCESTORS) > 0:
		return ANCESTORS

	_loadTermData()

	for childKey in _getStructureKeys():
		termAncestors = []

		parentKey = TERM[childKey][PARENT]
		while parentKey != None:
			termAncestors.append(parentKey)
			parentKey = TERM[parentKey][PARENT]

		ANCESTORS[childKey] = termAncestors

	logger.debug ('Got ancestors for %d terms' % len(ANCESTORS))
	return ANCESTORS

def _loadLeaves():
	global LEAVES

	if len(LEAVES) > 0:
		return LEAVES

	_loadTermData()
	_loadChildren()

	for structureKey in _getStructureKeys():
		if not CHILDREN.has_key(structureKey):
			LEAVES[structureKey] = 1

	logger.debug ('Got %d leaf terms' % len(LEAVES))
	return LEAVES

def _loadDescendents():
	global DESCENDENTS

	if len(DESCENDENTS) > 0:
		return DESCENDENTS

	query7 = '''select _Structure_key, _Descendent_key
		from GXD_StructureClosure'''

	(cols, rows) = dbAgnostic.execute (query7)

	structureCol = dbAgnostic.columnNumber (cols, '_Structure_key')
	descendentCol = dbAgnostic.columnNumber (cols, '_Descendent_key')

	for row in rows:
		structureKey = row[structureCol]
		descendentKey = row[descendentCol]

		if not DESCENDENTS.has_key(structureKey):
			DESCENDENTS[structureKey] = [ descendentKey ]
		else:
			DESCENDENTS[structureKey].append (descendentKey) 

	logger.debug ('Got descendents for %d AD terms' % len(DESCENDENTS))
	return DESCENDENTS

def _getStructureKeys():
	global STRUCTURE_KEYS

	if len(STRUCTURE_KEYS) > 0:
		return STRUCTURE_KEYS

	_loadTermData()

	STRUCTURE_KEYS = TERM.keys()
	STRUCTURE_KEYS.sort()

	return STRUCTURE_KEYS

def _isLeaf(structureKey):
	leaves = _loadLeaves()
	if leaves.has_key(structureKey):
		return 1
	return 0

def _morph (inputCols, inputRows, outputCols):
	# re-orders columns in 'inputRows', which come in with the ordering
	# from 'inputCols' and should go out with the ordering of
	# 'outputCols'.  Note that any name in 'outputCols' must also appear
	# in 'inputCols'.  It is okay to have fewer columns in 'outputCols'
	# than in 'inputCols', and it is okay to have the same column name
	# appear more than once in 'outputCols'.

	lowerInputCols = map (lambda x : x.lower(), inputCols)

	# has the position in 'inputCols' (and thus 'inputRows') of the 
	# columns specified in 'outputCols'
	desiredIndices = []

	for col in outputCols:
		if col in inputCols:
			desiredIndices.append (inputCols.index(col))
		elif col in lowerInputCols:
			desiredIndices.append (lowerInputCols.index(col))
		else:
			raise error, 'Unknown column in _morph: %s' % col

	outputRows = []

	for inputRow in inputRows:
		outputRow = []

		for i in desiredIndices:
			outputRow.append (inputRow[i])

		outputRows.append (outputRow)

	return outputRows

###--- Functions ---###

def getTermKey (structureKey):
	_loadTermData()
	if TERM.has_key(structureKey):
		return TERM[structureKey][TERMKEY]
	return None

def getMgiID(structureKey):
	_loadIDs()
	if MGI_ID.has_key(structureKey):
		return MGI_ID[structureKey]
	logger.debug ('Missing MGI ID for structure key: %d' % structureKey)
	return None

def getSequenceNum(structureKey):
	_loadTermData()
	if TERM.has_key(structureKey):
		return TERM[structureKey][SORT]
	return 0

def getStructureKey(termKey):
	_loadTermData()
	structureKey = termKey - AD_TERM_KEY_OFFSET
	if TERM.has_key(structureKey):
		return structureKey
	return 0

def getStructure (structureKey):
	_loadTermData()
	if TERM.has_key(structureKey):
		return TERM[structureKey][STRUCTURE]
	return None

def getStage (structureKey):
	_loadTermData()
	if TERM.has_key(structureKey):
		return TERM[structureKey][STAGE]
	return None

def getPrintname (structureKey):
	_loadTermData()
	if TERM.has_key(structureKey):
		return TERM[structureKey][PRINTNAME]
	return None

def getSystem (structureKey):
	_loadTermData()
	if TERM.has_key(structureKey):
		return TERM[structureKey][SYSTEM]
	return None

def getTermSequenceNumRows(outputColumns):
	# get rows for the term_sequence_num table
	# columns: term key, default sort, dfs sort, vocab-dag-term sort

	_loadConstants()
	_loadTermData()

	toSort = []

	for structureKey in _getStructureKeys():
		term = TERM[structureKey]

		toSort.append ( (term[STAGE], term[STRUCTURE].lower(),
			term[PRINTNAME].lower(), term[TERMKEY]) )

	toSort.sort(_vocabDagTermSort)

	i = AD_TERM_KEY_OFFSET
	vdtSort = {}
	for (stage, structure, printname, key) in toSort:
		i = i + 1
		vdtSort[key] = i

	logger.debug ('Sorted %d by stage, structure' % i) 

	rows = []
	cols = [ 'term_key', 'by_default', 'by_dfs', 'by_vocab_dag_alpha' ]

	for structureKey in _getStructureKeys():
		term = TERM[structureKey]

		row = [ term[TERMKEY],
			AD_TERM_KEY_OFFSET + term[SORT],
			AD_TERM_KEY_OFFSET + term[SORT],
			vdtSort[getTermKey(structureKey)]
			]
		rows.append (row)

	logger.debug ('Got %d seq num rows for AD structures' % len(rows))

	rows = _morph (cols, rows, outputColumns)
	logger.debug ('Returning %d AD rows for term_sequence_num table' % \
		len(rows))
	return rows

def getTermRows(outputColumns):
	# get rows for the term table.
	# columns:  term key, term, accID, vocab, display vocab, definition,
	#	sequence num, is root, is leaf, is obsolete

	_loadConstants()
	_loadTermData()
	_loadIDs()
	_loadDescendentCounts()

	myCols = [ 'termKey', 'term', 'accID', 'vocab', 'displayVocab', 'def',
		'sequenceNum', 'isRoot', 'isLeaf', 'isObsolete' ]

	rows = []

	for structureKey in _getStructureKeys():
		term = TERM[structureKey]

		row = []

		row.append (term[TERMKEY])
		row.append (term[STRUCTURE])

		if MGI_ID.has_key(structureKey):
			row.append(MGI_ID[structureKey])
		else:
			row.append(None)

		row.append (AD_VOCAB_NAME)
		row.append (AD_VOCAB_NAME)

		row.append (term[PRINTNAME])
		row.append (AD_TERM_KEY_OFFSET + term[SORT])

		if term[PARENT] == None:
			row.append (1)
		else:
			row.append (0)

		if DESCENDENT_COUNT.has_key(structureKey):
			if DESCENDENT_COUNT[structureKey] > 0:
				row.append(0)
			else:
				row.append(1)
		else:
			row.append(1)

		row.append(0)
		rows.append(row)

	rows = _morph (myCols, rows, outputColumns)
	logger.debug ('Returning %d AD rows for term table' % len(rows))
	return rows

def getVocabularyRows(outputColumns):
	# get rows for the vocabulary table.
	# columns:  vocab key, name, term count, is simple?, max depth

	_loadConstants()
	_loadTermData()

	myCols = [ '_Vocab_key', 'name', 'termCount', 'isSimple', 'maxDepth' ]

	rows = [ [ AD_VOCAB_KEY, AD_VOCAB_NAME, len(TERM), 0, MAX_DEPTH ] ]

	rows = _morph (myCols, rows, outputColumns)
	logger.debug ('Returning %d AD rows for vocabulary table' % len(rows))
	return rows

def getSynonymRows(outputColumns):
	# get rows for the term_synonym table.
	# columns:  term key, synonym, synonym type

	_loadTermData()
	_loadSynonyms()

	myCols = [ 'termKey', 'synonym', 'synonymType' ]

	rows = []

	for structureKey in _getStructureKeys():
		if SYNONYMS.has_key(structureKey):
			termKey = getTermKey(structureKey)

			for synonym in SYNONYMS[structureKey]:
				rows.append ( [ termKey, synonym, 'AD' ] )

	rows = _morph (myCols, rows, outputColumns)

	logger.debug ('Returning %d synonyms for AD terms' % len(rows))
	return rows

def getIDRows(outputColumns):
	# get rows for the term_id table.
	# columns:  term key, logical db, acc ID, preferred?, private?

	myCols = [ 'termKey', 'logicalDB', 'accID', 'preferred', 'private',
		'_LogicalDB_key' ]

	_loadTermData()
	_loadIDs()

	rows = []

	for structureKey in _getStructureKeys():
		if MGI_ID.has_key(structureKey):
			termKey = getTermKey(structureKey)

			rows.append ( [ termKey, 'MGI', MGI_ID[structureKey],
				1, 0, 1 ] )

	rows = _morph (myCols, rows, outputColumns)

	logger.debug ('Returning %d IDs for AD terms' % len(rows))
	return rows

def getTermAnatomyExtrasRows(outputColumns):
	# get rows for the term_anatomy_extras table.
	# columns:  term key, system, Theiler stage, structure key from mgd,
	#	edinburgh key

	_loadTermData()

	myCols = [ 'termKey', 'system', 'stage', 'structureKey',
		'edinburghKey' ]

	rows = []

	for structureKey in _getStructureKeys():
		term = TERM[structureKey]

		rows.append ( [ getTermKey(structureKey), term[SYSTEM],
			term[STAGE], structureKey, term[EDINBURGH] ] )

	rows = _morph (myCols, rows, outputColumns)
	logger.debug ('Returning extra data for %d AD terms' % len(rows))
	return rows

def getTermChildRows(outputColumns):
	# get rows for the term_child table.
	# columns:  term key, child term key, child term, child ID, 
	#	sequence num, is leaf, edge label

	_loadIDs()
	_loadTermData()
	_loadChildren()

	myCols = [ 'parentKey', 'childKey', 'term', 'accID', 'sequenceNum',
		'isLeaf', 'edgeLabel' ]

	rows = []

	for structureKey in _getStructureKeys():
		if CHILDREN.has_key(structureKey):
			termKey = getTermKey(structureKey)

			for childKey in CHILDREN[structureKey]:
				child = TERM[childKey]

				row = [ termKey,
					getTermKey(childKey),
					child[STRUCTURE],
					getMgiID(childKey),
					getSequenceNum(childKey),
					_isLeaf(childKey),
					EDGE_LABEL,
					]
				rows.append(row)

	rows = _morph (myCols, rows, outputColumns)
	logger.debug ('Returning %d child rows for AD terms' % len(rows))
	return rows

def getTermSiblingRows(outputColumns):
	# get rows for the term_sibling table.
	# columns:  term key, sibling term key, sibling term, sibling ID,
	#	sequence num, is leaf, edge label, path number

	_loadIDs()
	_loadTermData()
	_loadChildren()

	myCols = [ 'termKey', 'siblingKey', 'term', 'accID', 'sequenceNum',
		'isLeaf', 'edgeLabel', 'pathNumber' ]

	rows = []

	for structureKey in _getStructureKeys():
		parentKey = TERM[structureKey][PARENT]

		if parentKey != None:
			termKey = getTermKey (structureKey)

			for siblingKey in CHILDREN[parentKey]:

				# if we found the starting term, skip it
				if siblingKey == structureKey:
					continue

				sibling = TERM[siblingKey]

				row = [ termKey,
					getTermKey(siblingKey),
					sibling[STRUCTURE],
					getMgiID(siblingKey),
					getSequenceNum(siblingKey),
					_isLeaf(siblingKey),
					EDGE_LABEL,
					1,
					]
				rows.append (row) 

	rows = _morph (myCols, rows, outputColumns)
	logger.debug ('Returning %d sibling rows for AD terms' % len(rows))
	return rows

def getTermDescendentRows(outputCols):
	# get rows for the term_descendent table.
	# columns:  term key, descendent term key, descendent term,
	#	descendent ID, sequence num

	_loadIDs()
	_loadTermData()

	myCols = [ 'parentKey', 'childKey', 'term', 'accID', 'sequenceNum' ]

	descendents = _loadDescendents()

	rows = []

	for structureKey in _getStructureKeys():
		if not descendents.has_key(structureKey):
			continue

		parentKey = getTermKey (structureKey)

		for descendentKey in descendents[structureKey]:
			descendent = TERM[descendentKey]

			row = [ parentKey,
				getTermKey(descendentKey),
				descendent[STRUCTURE],
				getMgiID(descendentKey),
				getSequenceNum(descendentKey),
				]
			rows.append (row)

	rows = _morph (myCols, rows, outputCols)

	logger.debug ('Returning %d descendent rows for AD terms' % len(rows))
	return rows

def getTermAncestorRows(outputColumns):
	# get rows for the term_ancestor table.
	# columns:  term key, ancestor term key, ancestor term,	ancestor ID,
	#	path number, depth, edge label

	ancestors = _loadAncestors()

	myCols = [ 'termKey', 'ancestorTermKey', 'ancestorTerm', 'ancestorID',
		'pathNumber', 'depth', 'edgeLabel' ]

	rows = []

	for structureKey in _getStructureKeys():
		if not ancestors.has_key(structureKey):
			continue

		childKey = getTermKey(structureKey)

		for ancestorKey in ancestors[structureKey]:
			ancestor = TERM[ancestorKey]

			row = [ childKey,
				getTermKey(ancestorKey),
				ancestor[STRUCTURE],
				getMgiID(ancestorKey),
				1,
				ancestor[DEPTH],
				EDGE_LABEL,
				]
			rows.append(row)

	rows = _morph (myCols, rows, outputColumns)
	logger.debug ('Returning %d ancestor rows for AD terms' % len(rows))
	return rows

def getTermCountsRows(outputColumns):
	# get rows for the term_counts table.
	# columns:  term key, path count, descendent count, child count,
	#	marker count, expression marker count, gxdlit marker count

	descendentCounts = _loadDescendentCounts()
	children = _loadChildren()

	myCols = [ 'termKey', 'pathCount', 'descendentCount', 'childCount',
		'markerCount', 'expressionMarkerCount', 'gxdLitMarkerCount' ]
	rows = []

	for structureKey in _getStructureKeys():
		descendentCount = 0
		childrenCount = 0

		if descendentCounts.has_key(structureKey):
			descendentCount = descendentCounts[structureKey]

		if children.has_key(structureKey):
			childrenCount = len(children[structureKey])

		rows.append ( [ getTermKey(structureKey), 1,
			descendentCount, childrenCount, 0, 0, 0 ] )

	rows = _morph (myCols, rows, outputColumns)
	logger.debug ('Returning %d term count rows for AD terms' % len(rows))
	return rows

def getTermAnnotationCountsRows(outputColumns):
	# get rows for the term_annotation_counts table.
	# columns:  term key, annotated object type, object count for term
	#	only, object count for term plus descendents, annotation
	#	count for term only, annotation count for term plus
	#	descendents

	myCols = [ 'termKey', 'mgitype', 'objectsToTerm',
		'objectsWithDesc', 'annotToTerm', 'annotWithDesc' ]

	rows = []

	# TBD - insert logic here to compute any needed counts

	rows = _morph (myCols, rows, outputColumns)
	logger.debug ('Returning %d term annotation counts rows for AD terms'\
		% len(rows))
	return rows

