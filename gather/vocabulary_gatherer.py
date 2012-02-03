#!/usr/local/bin/python
# 
# gathers data for the 'vocabulary' table in the front-end database

import Gatherer
import logger

###--- Functions ---###

depthCache = {}		# cache of max depth for each term key

def maxDepth (term, vocab, edges):
	global depthCache

	# if we've already found and cached this, just return it

	if depthCache.has_key(term):
		return depthCache[term]

	# if this term has no children, its depth is simply 1

	if not edges[vocab].has_key(term):
		depthCache[term] = 1
		return 1

	# recursively iterate through this term's children to find the
	# maximum depth

	deepest = 0
	for (edgeType, child) in edges[vocab][term]:
		childDepth = maxDepth(child, vocab, edges)
		deepest = max(deepest, childDepth)

	depthCache[term] = deepest + 1 
	return deepest + 1

pathsUpward = {}	# cache of paths upward for each term key

def pathsToRoots (term, vocab, upEdges):
	global pathsUpward

	# if we've already found and cached this, just return it

	if pathsUpward.has_key (term):
		return pathsUpward[term]

	# if this term has no ancestors, it is itself a root

	if (not upEdges.has_key(vocab)) or (not upEdges[vocab].has_key(term)):
		pathsUpward[term] = [ [ (None, term) ] ]
		return pathsUpward[term]

	# recursively iterate through this term's ancestors to enumerate all
	# possible paths up to roots

	paths = []
	for (edgeType, ancestor) in upEdges[vocab][term]:
		ancestorPaths = pathsToRoots (ancestor, vocab, upEdges)
		for p in ancestorPaths:
			pNew = p[:]
			pNew.append ( (edgeType, term) )
			paths.append (pNew)

	pathsUpward[term] = paths
	return paths

###--- Classes ---###

class VocabularyGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the vocabulary table and certain term tables
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for vocabularies,
	#	collates results, writes tab-delimited text file

	def findVocabularies (self):
		# edges[vocab] = {parent : [ (edge type 1, child 1), ... ]}
		edges = {}

		# upEdges[vocab] = { child : [ (edge type 1, parent 1), ... ]}
		upEdges = {}

		# isRoot[vocab][term key] = True/False
		isRoot = {}

		# vocabs[term key] = vocab key
		vocabs = {}

		# query 0 - capture the parent/child relationships in memory

		vocCol = Gatherer.columnNumber (self.results[0][0],
			'_Vocab_key')
		pCol = Gatherer.columnNumber (self.results[0][0], 'parentKey')
		cCol = Gatherer.columnNumber (self.results[0][0], 'childKey')
		eCol = Gatherer.columnNumber (self.results[0][0],
			'_Label_key')

		for row in self.results[0][1]:
			voc = row[vocCol]
			parent = row[pCol]
			child = row[cCol]
			edgeType = Gatherer.resolve (row[eCol], 'dag_label',
				'_Label_key', 'label')

			if not edges.has_key(voc):
				edges[voc] = { parent : [ (edgeType, child) ]}
			elif not edges[voc].has_key(parent):
				edges[voc][parent] = [ (edgeType, child) ]
			else:
				edges[voc][parent].append ( (edgeType, child) )

			if not upEdges.has_key(voc):
				upEdges[voc] = { child : [ (edgeType, parent)
					] }
			elif not upEdges[voc].has_key(child):
				upEdges[voc][child] = [ (edgeType, parent) ]
			else:
				upEdges[voc][child].append ( (edgeType,
					parent) )

			# existing as a child term ensures that this term is
			# not a root term

			if not isRoot.has_key (voc):
				isRoot[voc] = {}

			isRoot[voc][child] = False

			# if we have a parent term we have not seen before (as
			# either a parent or a child), then assume it is a
			# root until we discover differently

			if not isRoot[voc].has_key (parent):
				isRoot[voc][parent] = True

			# cache the vocabulary for each term

			vocabs[child] = voc
			vocabs[parent] = voc

		logger.debug ('Read data for %d edges, %d terms' % (
			len(self.results[0][1]), len(vocabs)) )

		# now examine those edges to find the longest path for each
		# root term to its leaves

		# vocDepth[vocab] = max depth for that vocab
		vocDepth = {}

		vocabList = isRoot.keys()
		for vocab in vocabList:
			vocDepth[vocab] = 0
			terms = isRoot[vocab].keys()
			for term in terms:
				if isRoot[vocab][term] == False:
					continue
				depth = maxDepth (term, vocab, edges)
				vocDepth[vocab] = max(vocDepth[vocab], depth)

		logger.debug ('Found maximum depths for %d vocabs' % \
			len(isRoot))

		# identify the rows and columns for this data set

		columns = self.results[2][0]
		rows = map (list, self.results[2][1])

		# we need to add a 'maxDepth' column to each row

		vocCol = Gatherer.columnNumber (self.results[2][0],
			'_Vocab_key')
		columns.append ('maxDepth')

		for row in rows:
			voc = row[vocCol]
			if vocDepth.has_key(voc):
				depth = vocDepth[voc]
			else:
				depth = 1

			row.append (depth)

		logger.debug ('Compiled %d vocab rows' % len(rows))

		return edges, upEdges, isRoot, vocabs, columns, rows

	def collectIDs (self):
		ids = {}

		keyCol = Gatherer.columnNumber (self.results[1][0],
			'_Object_key')
		idCol = Gatherer.columnNumber (self.results[1][0], 'accID')

		for row in self.results[1][1]:
			key = row[keyCol]
			if not ids.has_key (key):
				ids[key] = row[idCol]

		logger.debug ('Cached %d IDs for terms' % len(ids))
		return ids

	def findChildren (self, ids, edges, sequenceNum):

		vCol = Gatherer.columnNumber (self.results[0][0],
			'_Vocab_key')
		pCol = Gatherer.columnNumber (self.results[0][0], 'parentKey')
		cCol = Gatherer.columnNumber (self.results[0][0], 'childKey')
		tCol = Gatherer.columnNumber (self.results[0][0], 'term')
		sCol = Gatherer.columnNumber (self.results[0][0],
			'sequenceNum')
		eCol = Gatherer.columnNumber (self.results[0][0],
			'_Label_key')

		columns = [ 'parentKey', 'childKey', 'term', 'accID',
			'sequenceNum', 'isLeaf', 'edgeLabel' ]
		rows = []

		for r in self.results[0][1]:
			row = [ r[pCol], r[cCol], r[tCol] ]
			if ids.has_key(r[cCol]):
				row.append (ids[r[cCol]])
			else:
				row.append (None)

			# some vocabs have a defined ordering.  if this terms
			# is ordered, then use that one.  if not, use our
			# computed sequence number.

			if r[sCol]:
				row.append (r[sCol])
			else:
				row.append (sequenceNum[r[cCol]])

			isLeaf = 1
			if edges.has_key(r[vCol]):
				if edges[r[vCol]].has_key(r[cCol]):
					isLeaf = 0
			row.append (isLeaf)

			edgeType = Gatherer.resolve (r[eCol], 'dag_label',
				'_Label_key', 'label')
			row.append (edgeType)

			rows.append (row)

		logger.debug ('Found %d parent/child pairs' % len(rows))
		return columns, rows

	def collectDefinitions (self):
		keyCol = Gatherer.columnNumber (self.results[3][0], '_Term_key')
		noteCol = Gatherer.columnNumber (self.results[3][0], 'note')

		defs = {}		# defs[term key] = definition

		for row in self.results[3][1]:
			key = row[keyCol]
			if defs.has_key (key):
				defs[key] = defs[key] + row[noteCol]
			else:
				defs[key] = row[noteCol]

		# trim trailing whitespace

		for key in defs.keys():
			defs[key] = defs[key].rstrip()

		# trim trailing backslash (uncommon, but causes errors)
		for key in defs.keys():
			if defs[key][-1] == '\\':
				defs[key] = defs[key][:-1]

		logger.debug ('Cached %d term definitions' % len(defs))
		return defs

	def findTerms (self, ids, defs, isRoot, edges, goOntologies):
		kCol = Gatherer.columnNumber (self.results[4][0], '_Term_key')
		vCol = Gatherer.columnNumber (self.results[4][0],
			'_Vocab_key')
		tCol = Gatherer.columnNumber (self.results[4][0], 'term')
		sCol = Gatherer.columnNumber (self.results[4][0],
			'sequenceNum')
		isObsoleteCol = Gatherer.columnNumber (self.results[4][0],
			'isObsolete')

		terms = {}		# terms[term key] = term

		columns = [ 'termKey', 'term', 'accID', 'vocab',
			'displayVocab', 'def', 'sequenceNum', 'isRoot',
			'isLeaf', 'isObsolete' ]

		# produce a sorted list of terms, for those vocabularies
		# without a pre-assigned set of sequence numbers

		toSort = []		# (term, term key)...
		for r in self.results[4][1]:
			if r[tCol]:
				toSort.append ( (r[tCol].lower(), r[kCol]) )
			else:
				toSort.append ( ('', r[kCol]) )

		toSort.sort()
		i = 0
		sequenceNum = {}		# sequenceNum[termKey] = i

		for (term, termKey) in toSort:
			i = i + 1
			sequenceNum[termKey] = i

		logger.debug ('Sorted %d terms' % len(sequenceNum))

		# now compile the rows for the term table

		rows = []
		for r in self.results[4][1]:
			key = r[kCol]
			voc = r[vCol]

			row = [ key, r[tCol] ]

			terms[key] = r[tCol]

			if ids.has_key(key):
				row.append (ids[key])
			else:
				row.append (None)

			# We need both the raw vocab name and, for some cases,
			# a special display-only vocab name.

			vocabName = Gatherer.resolve (voc, 'voc_vocab',
				'_Vocab_key', 'name')

			row.append (vocabName)

			if goOntologies.has_key(key):
				row.append (goOntologies[key])
			else:
				row.append (vocabName)

			if defs.has_key (key):
				row.append (defs[key])
			else:
				row.append (None)

			# some vocabularies have a defined ordering; if this
			# term has such an ordering, use it; if not, use our
			# computed ordering

			if r[sCol] != None:
				row.append (r[sCol])
			else:
				row.append (sequenceNum[key])

			flag = 0
			if isRoot.has_key(voc):
				if isRoot[voc].has_key (key):
					if isRoot[voc][key]:
						flag = 1
			row.append (flag)

			flag = 0
			if edges.has_key(voc):
				if not edges[voc].has_key(key):
					flag = 1
			row.append (flag)

			row.append (r[isObsoleteCol])
			rows.append (row)

		logger.debug ('Found %d terms' % len(rows))

		return terms, sequenceNum, columns, rows

	def collectDescendentCounts (self):
		keyCol = Gatherer.columnNumber (self.results[5][0],
			'_AncestorObject_key')
		ctCol = Gatherer.columnNumber (self.results[5][0], 'ct')

		counts = {}
		for row in self.results[5][1]:
			counts[row[keyCol]] = row[ctCol]
		logger.debug ('Found %d descendent counts' % len(counts))
		return counts

	def findTermCounts (self, descendentCounts, edges, upEdges):
		kCol = Gatherer.columnNumber (self.results[4][0], '_Term_key')
		vCol = Gatherer.columnNumber (self.results[4][0],
			'_Vocab_key')

		rows = []
		columns = [ 'termKey', 'pathCount', 'descendentCount',
			'childCount' ]

		for r in self.results[4][1]:
			key = r[kCol]
			vocab = r[vCol]

			paths = pathsToRoots (key, vocab, upEdges)
			row = [ key, len(paths) ]

			if descendentCounts.has_key(key):
				row.append (descendentCounts[key])
			else:
				row.append (0)

			if edges.has_key(vocab) and edges[vocab].has_key(key):
				row.append (len(edges[vocab][key]))
			else:
				row.append (0)
			rows.append (row)

		logger.debug ('Got %d term_counts' % len(rows))
		return columns, rows

	def findTermAncestors (self, upEdges, ids, termByKey):
		columns = [ 'termKey', 'ancestorTermKey', 'ancestorTerm',
			'ancestorID', 'pathNumber', 'depth', 'edgeLabel' ]
		rows = []

		# pathCache[term] = { parent term key : path number }
		pathCache = {}

		# for each vocab, walk through its terms

		vocabs = upEdges.keys()
		for voc in vocabs:
			terms = upEdges[voc].keys()

			# for each term, find all its paths up to root terms

			for term in terms:
				paths = pathsToRoots (term, voc, upEdges)
				pathNum = 0

				pathCache[term] = {}

				# for each path, enumerate all terms along the
				# path

				for path in paths:
					depth = 0
					pathNum = pathNum + 1

					# cache the parent and its path number
					if len(path) > 1:
						pathCache[term][path[-2]] = \
							pathNum

					# for each ancestor, add a record to
					# the set of rows

					for (edgeType, ancestor) in path:

						# if we reach the current term
						# then we don't need a record
						# for it

						if ancestor == term:
							continue

						depth = depth + 1
						row = [ term, ancestor,
							termByKey[ancestor]
							]

						if ids.has_key(ancestor):
							row.append (
								ids[ancestor])
						else:
							row.append (None)

						row.append (pathNum)
						row.append (depth)
						row.append (edgeType)
						rows.append (row)
		logger.debug ('Found %d ancestors' % len(rows))
		return pathCache, columns, rows

	def extractCountsToTerm (self):

		# first, compile unique sets of objects and annotations from
		# the query

		columns, rows = self.results[6]

		tKey = Gatherer.columnNumber (columns, '_Term_key')
		mKey = Gatherer.columnNumber (columns, '_MGIType_key')
		oKey = Gatherer.columnNumber(columns, '_Object_key')
		eKey = Gatherer.columnNumber(columns, '_EvidenceTerm_key')
		qKey = Gatherer.columnNumber(columns, '_Qualifier_key')

		# objects[term key] = { mgitype key : { object key : 1 } }
		objects = {}

		# annots[term key] = { mgitype key : { annotation : 1 } }
		annots = {}

		for row in rows:
			key = row[tKey]
			object = row[oKey]
			mgitype = row[mKey]

			# an annotation is defined as a unique set of:
			#	term, object, evidence term, qualifier
			annotation = (key, object, row[eKey], row[qKey])

			if not objects.has_key (key):
				objects[key] = { mgitype : { object : 1 } }
				annots[key] = { mgitype : { annotation : 1 } }

			elif not objects[key].has_key (mgitype):
				objects[key][mgitype] = { object : 1 }
				annots[key][mgitype] = { annotation : 1 }
			else:
				objects[key][mgitype][object] = 1
				annots[key][mgitype][annotation] = 1

		# then, pull those unique sets into their respective counts

		# dict[term key] = { mgi type key : (obj count, annot count) }
		dict = {}

		termKeys = objects.keys()
		for term in termKeys:
			mgitypes = objects[term].keys()

			for mgitype in mgitypes:
				if dict.has_key(term):
					dict[term][mgitype] = (
						len(objects[term][mgitype]),
						len(annots[term][mgitype]) )
				else:
					dict[term] = { mgitype : (
						len(objects[term][mgitype]),
						len(annots[term][mgitype]) ) }

		logger.debug ('Extracted counts to term')
		return dict

	def extractCountsDownDAG (self):

		# first, compile the list of ancestors for each term

		columns, rows = self.results[7]

		aKey = Gatherer.columnNumber(columns, '_AncestorObject_key')
		dKey = Gatherer.columnNumber(columns, '_DescendentObject_key')

		# ancestors[descendent key] = [ ancestor key 1, ... a.k. n ]
		ancestors = {}

		for r in rows:
			ancestorKey = r[aKey]
			descendentKey = r[dKey]

			if ancestors.has_key(descendentKey):
				ancestors[descendentKey].append (ancestorKey)
			else:
				ancestors[descendentKey] = [ ancestorKey ]

		logger.debug ('Found %d ancestors for %d terms' % (
			len(rows), len(ancestors)) )

		# second, go through each annotation.  for each annotation and
		# each annotated object, add them to the set of each for the
		# term

		columns, rows = self.results[8]

		tKey = Gatherer.columnNumber(columns, '_Term_key')
		oKey = Gatherer.columnNumber(columns, '_Object_key')
		mKey = Gatherer.columnNumber(columns, '_MGIType_key')
		eKey = Gatherer.columnNumber(columns, '_EvidenceTerm_key')
		qKey = Gatherer.columnNumber(columns, '_Qualifier_key')

		# note that for the purposes of our annotation count, we
		# define a distinct annotation to be a unique set of:
		# 	object, term, qualifier, evidence term

		# objects[term key] = { mgitype key : { object key : 1 } }
		objects = {}

		# annots[term key] = { mgitype key : {
		#    (object key, qualifier key, evidence term key) : 1 } }
		annots = {}

		for r in rows:
			termKey = r[tKey]
			mgitypeKey = r[mKey]
			objectKey = r[oKey]
			qualifierKey = r[qKey]
			evidenceTermKey = r[eKey]

			annot = (termKey, objectKey, qualifierKey,
				evidenceTermKey)

			# track which objects are associated with which terms
			# and each term's ancestors (do likewise for
			# annotation keys)

			if ancestors.has_key (termKey):
				toDo = [ termKey ] + ancestors[termKey]
			else:
				toDo = [ termKey ]

			for t in toDo:
			    if not objects.has_key(t):
				objects[t] = {mgitypeKey : { objectKey : 1 }}
				annots[t] = {mgitypeKey : { annot : 1 }}

			    elif not objects[t].has_key (mgitypeKey):
				objects[t][mgitypeKey] = { objectKey : 1 }
				annots[t][mgitypeKey] = { annot : 1 }

			    else:
				objects[t][mgitypeKey][objectKey] = 1
				annots[t][mgitypeKey][annot] = 1

		logger.debug ('Found objects and annotations for %d terms' \
			% len(objects))

		# third, compile those objects and annotations for each term
		# into their respective counts

		# dict[term key] = { mgi type key : (obj count, annot count) }
		dict = {}

		termKeys = objects.keys()
		for termKey in termKeys:
			mgitypes = objects[termKey].keys()
			dict[termKey] = {}

			for mgitype in mgitypes:
				dict[termKey][mgitype] = (
					len(objects[termKey][mgitype]),
					len(annots[termKey][mgitype]) )

		logger.debug ('Collated DAG counts for %d terms' % len(dict))
		return dict

	def findAnnotationCounts (self):
		columns = [ 'termKey', 'mgitype', 'objectsToTerm',
			'objectsWithDesc', 'annotToTerm', 'annotWithDesc' ]
		rows = []

		toTerm = self.extractCountsToTerm()
		#logger.debug ('Extracted counts to term')

		withDescendents = self.extractCountsDownDAG()
		#logger.debug ('extracted counts with descendents')
		
		# get unified list of term keys which have annotations to
		# themselves directly, or to their descendents, or both

		termKeys = toTerm.keys()
		for key in withDescendents.keys():
			if not toTerm.has_key(key):
				termKeys.append (key) 

		for term in termKeys:
			mgitypes = []

			if toTerm.has_key(term):
				mgitypes = toTerm[term].keys()

			if withDescendents.has_key(term):
				for mgitype in withDescendents[term].keys():
					if mgitype not in mgitypes:
						mgitypes.append (mgitype)

			for mgitype in mgitypes:
				typeText = Gatherer.resolve (mgitype,
					'acc_mgitype', '_MGIType_key', 'name')

				obj1 = 0
				anno1 = 0
				obj2 = 0
				anno2 = 0

				if toTerm.has_key(term):
					if toTerm[term].has_key(mgitype):
						obj1, anno1 = \
							toTerm[term][mgitype]

				if withDescendents.has_key(term):
					if withDescendents[term].has_key (
						mgitype):
						obj2, anno2 = \
						withDescendents[term][mgitype]

				row = [ term, typeText, obj1, obj2, anno1,
					anno2 ]
				rows.append (row)
			
		logger.debug ('Collated %d rows for annot counts' % len(rows))
		return columns, rows

	def findSiblings (self, pathCache, terms, ids, edges, vocabs,
			sequenceNum):
		termList = pathCache.keys()
		columns = [ 'termKey', 'siblingKey', 'term', 'accID',
			'sequenceNum', 'isLeaf', 'edgeLabel', 'pathNumber' ]
		rows = []

		for term in termList:
			ancestorDict = pathCache[term]
			vocabKey = vocabs[term]

			for ((edgeType, ancestor), pathNum) in \
				ancestorDict.items():

				for (edgeType, sibling) in \
					edges[vocabKey][ancestor]:

					if sibling == term:
						continue

					isLeaf = 1
					if edges[vocabKey].has_key(sibling):
						isLeaf = 0

					termText = None
					termID = None
					if terms.has_key(sibling):
						termText = terms[sibling]
					if ids.has_key(sibling):
						termID = ids[sibling]

					row = [ term, sibling, termText,
						termID, sequenceNum[sibling],
						isLeaf, edgeType, pathNum ]
					rows.append (row)
		logger.debug ('Collated %d rows for siblings' % len(rows))
		return columns, rows

	def getGOOntologies (self):
		columns, rows = self.results[9]

		tKey = Gatherer.columnNumber (columns, '_Term_key')
		ontologyKey = Gatherer.columnNumber (columns, 'dagShorthand')

		go = {}
		for row in rows:
			go[row[tKey]] = row[ontologyKey]

		return go

	def collateResults (self):

		# step 1 -- vocabulary table

		edges, upEdges, isRoot, vocabs, columns, rows = \
			self.findVocabularies()
		self.output.append ( (columns, rows) )

		# step 2 -- cache primary ID for each term

		ids = self.collectIDs()

		# step 3 -- get special display overrides for GO terms
		goOntologies = self.getGOOntologies()

		# step 4 -- cache term definitions
		
		defs = self.collectDefinitions()

		# step 5 -- term table

		terms, sequenceNum, columns, rows = self.findTerms (ids, defs,
			isRoot, edges, goOntologies)
		self.output.append ( (columns, rows) )

		# step 3 -- term_child table (moved to 5a)

		columns, rows = self.findChildren (ids, edges, sequenceNum)
		self.output.append ( (columns, rows) )

		# step 6 -- get counts of descendents for each term

		descendentCounts = self.collectDescendentCounts()

		# step 7 -- term_counts table

		columns, rows = self.findTermCounts (descendentCounts, edges,
			upEdges)
		self.output.append ( (columns, rows) )

		# step 8 -- term_ancestor table

		pathCache, columns, rows = self.findTermAncestors (upEdges,
			ids, terms)
		self.output.append ( (columns, rows) )

		# step 9 -- term_annotation_counts table
		columns, rows = self.findAnnotationCounts ()
		self.output.append ( (columns, rows) )

		# step 10 -- term_sibling table
		columns, rows = self.findSiblings (pathCache, terms, ids,
			edges, vocabs, sequenceNum)
		self.output.append ( (columns, rows) ) 
		return

###--- globals ---###

cmds = [
	# query 0 - parent/child relationships
	'''select t._Vocab_key,
			p._Object_key as parentKey,
			c._Object_key as childKey,
			ct.term,
			ct.sequenceNum,
			e._Label_key
		from voc_term t,
			dag_node p,
			dag_node c,
			dag_edge e,
			voc_term ct
		where t._Term_key = p._Object_key
			and p._Node_key = e._Parent_key
			and c._Object_key = ct._Term_key
			and e._Child_key = c._Node_key''',

	# query 1 - IDs, sorted by _LogicalDB_key to bring MGI IDs to the top
	'''select _Object_key, accID, _LogicalDB_key
		from acc_accession
		where _MGIType_key = 13
			and preferred = 1
			and private = 0
		order by _LogicalDB_key''',

	# query 2 - vocabularies
	'''select v._Vocab_key, v.isSimple, v.name, count(1) as termCount
		from voc_vocab v,
			voc_term t
		where v._Vocab_key = t._Vocab_key
		group by v._Vocab_key, v.isSimple, v.name''',

	# query 3 - term definitions
	'''select _Term_key, sequenceNum, note
		from voc_text
		order by sequenceNum''',

	# query 4 - terms
	'''select _Term_key, _Vocab_key, term, sequenceNum, isObsolete
		from voc_term''',

	# query 5 - descendent counts
	'''select _AncestorObject_key, count(1) as ct
		from DAG_Closure
		group by _AncestorObject_key''',

	# query 6 - annotation counts
	'''select a._Term_key,
			t._MGIType_key,
			a._Object_key,
			e._EvidenceTerm_key,
			a._Qualifier_key
		from voc_annot a, voc_annottype t, voc_evidence e
		where a._AnnotType_key = t._AnnotType_key
			and a._Annot_key = e._Annot_key''',

	# query 7 - ancestor/descendent relationships from the DAG
	'''select _AncestorObject_key, _DescendentObject_key
		from dag_closure''',

	# query 8 - all annotations, so we can get counts "down the DAG".
	 '''select a._Term_key,
			t._MGIType_key,
			a._Object_key,
			e._EvidenceTerm_key,
			a._Qualifier_key
		from voc_annot a, voc_annottype t, voc_evidence e
		where a._AnnotType_key = t._AnnotType_key
			and a._Annot_key = e._Annot_key''',

	# query 9 - shorthand notation for display (instead of vocab name)
	# for GO terms
	'''select t._Term_key, case
			when d.abbreviation = 'C' then 'Component'
			when d.abbreviation = 'F' then 'Function'
			when d.abbreviation = 'P' then 'Process'
			when d.abbreviation = 'O' then 'Obsolete'
			else 'Gene Ontology'
			end as dagShorthand
		from voc_term t, dag_node n, dag_dag d
		where t._Term_key = n._Object_key 
			and n._DAG_key = d._DAG_key
			and t._Vocab_key = 4''',
	]

files = [
	('vocabulary',
		[ '_Vocab_key', 'name', 'termCount', 'isSimple', 'maxDepth' ],
		'vocabulary'),

	('term',
		[ 'termKey', 'term', 'accID', 'vocab', 'displayVocab', 'def',
			'sequenceNum', 'isRoot', 'isLeaf', 'isObsolete' ],
		'term'),

	('term_child',
		[ Gatherer.AUTO, 'parentKey', 'childKey', 'term', 'accID',
			'sequenceNum', 'isLeaf', 'edgeLabel' ],
		'term_child'),

	('term_counts',
		[ 'termKey', 'pathCount', 'descendentCount', 'childCount' ],
		'term_counts'),

	('term_ancestor',
		[ Gatherer.AUTO, 'termKey', 'ancestorTermKey', 'ancestorTerm',
			'ancestorID', 'pathNumber', 'depth', 'edgeLabel' ],
		'term_ancestor'),

	('term_annotation_counts',
		[ Gatherer.AUTO, 'termKey', 'mgitype', 'objectsToTerm',
			'objectsWithDesc', 'annotToTerm', 'annotWithDesc' ],
		'term_annotation_counts'),

	('term_sibling',
		[ Gatherer.AUTO, 'termKey', 'siblingKey', 'term', 'accID',
			'sequenceNum', 'isLeaf', 'edgeLabel', 'pathNumber' ],
		'term_sibling'),
	]

# global instance of a VocabularyGatherer
gatherer = VocabularyGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
