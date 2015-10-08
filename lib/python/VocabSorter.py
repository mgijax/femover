# Module: VocabSorter.py
# Purpose: to provide sorting of vocabulary terms, using a depth-first search
#	(DFS) algorithm within DAG-based vocabularies.  For sibling terms, we
#	prioritize them by its sequence number from the database (if
#	available) or alphabetically if not.  Each term is given its new
#	sequence number when it is first visited by the DFS algorithm, so in
#	the case of a term with multiple parent nodes, only the first one
#	affects its ordering.  For flat vocabularies, the terms are ordered as
#	for siblings of a DAG-based vocabulary (the sequence number from the
#	database, then alphabetically).

import dbAgnostic
import logger
import symbolsort

###--- Globals ---###

VOCAB_SORTER = None		# singleton instance to be shared
VOCAB_SORTER_ALPHA = None

VOCABS = ''

###--- PrivateFunctions ---###

def __initializeVS():
	global VOCAB_SORTER
	if not VOCAB_SORTER:
		VOCAB_SORTER = VocabSorter()
	return

def __initializeVSA():
	global VOCAB_SORTER_ALPHA
	if not VOCAB_SORTER_ALPHA:
		VOCAB_SORTER_ALPHA = VocabSorterAlpha()
	return

def _vocabNameCmp (a, b):
	# compare two vocabulary names to order them

	if a is None:
		if b is None:
			return 0
		return -1

	elif b is None:
		return 1

	return cmp (a.lower(), b.lower())

def _dagNameCmp (a, b):
	# compare two DAG names to order them

	if a is None:
		if b is None:
			return 0
		return -1

	elif b is None:
		return 1

	aLower = a.lower()
	bLower = b.lower()

	# special ordering for the three GO DAGs; alphabetical otherwise

	if aLower == 'molecular function':
		aLower = 'aaaa'
	elif aLower == 'cellular component':
		aLower = 'bbbb'
	elif aLower == 'biological process':
		aLower = 'cccc'

	if bLower == 'molecular function':
		bLower = 'aaaa'
	elif bLower == 'cellular component':
		bLower = 'bbbb'
	elif bLower == 'biological process':
		bLower = 'cccc'

	return cmp(aLower, bLower)

def _smartTermCmp (a, b):
	# compare two terms, ordering embedded integers correctly as well

	if a is None:
		if b is None:
			return 0
		return -1

	elif b is None:
		return 1

	aLower = a.lower()
	bLower = b.lower()

	return symbolsort.nomenCompare(aLower, bLower)

def _prepareOrderingDictionary (cmd, fn):
	# execute the command, order the results, and return as a dictionary
	#	{ key : sequence num }
	# ordering of terms according to given function 'fn'

	out = {}

	(cols, rows) = dbAgnostic.execute (cmd)

	keyCol = dbAgnostic.columnNumber (cols, 'key')
	nameCol = dbAgnostic.columnNumber (cols, 'name')

	nameToKeys = {}		# name -> [ key 1, key 2, ... ]

	for row in rows:
		name = row[nameCol]

		if nameToKeys.has_key(name):
			nameToKeys[name].append (row[keyCol])
		else:
			nameToKeys[name] = [ row[keyCol] ]

	names = nameToKeys.keys()	# list of all names
	names.sort(fn)

	i = 0
	for name in names:
		i = i + 1
		for key in nameToKeys[name]:
			out[key] = i

	return out

###--- Classes ---###

class VocabSorter:
	# manages sorting terms by depth-first search (DFS)

	def __init__ (self):
		# maps from (vocab name, term) to the respective term key
		self.termKeys = {}

		# maps from term key to the term's default ordering
		self.defaultOrder = {}

		# maps from term key to list of its descendent term keys
		self.children = {}

		# dictionary of term keys for root terms
		#	root term key -> dag key
		self.roots = {}

		# maps from a term key to all of its ancestors
		# 	term key -> { ancestor term key : 1 }
		self.ancestors = {}

		# dictionary of term keys which are not root terms
		self.notRoots = {}

		# maps from term keys to their final, DAG-considered ordering
		self.finalOrder = {}

		# load from database
		self.__getTermKeysAndDefaultOrder()
		self.__getDAGs()

		# order the children of each parent node
		self.__orderChildren()
		
		# compute the final ordering, using a DFS algorithm on the DAG
		# vocabularies
		self.__computeFinalOrder()

		# get the transitive closure (pairs of ancestors and
		# descendents) from the database
		self.__getTransitiveClosure()
		return

	###--- public methods ---###

	def sequenceNum (self, termKey):
		if self.finalOrder.has_key(termKey):
			return self.finalOrder[termKey]
		return None

	def getTermKey (self, vocabName, term):
		if self.termKeys.has_key ( (vocabName, term) ):
			return self.termKeys ( (vocabName, term) )
		return None

	def isChildOf (self, childTerm, parentTerm):
		if self.children.has_key(parentTerm):
			if childTerm in self.children[parentTerm]:
				return 1
		return 0

	def isDescendentOf (self, descendentTerm, ancestorTerm):
		if self.ancestors.has_key(descendentTerm):
			if self.ancestors[descendentTerm].has_key (
				ancestorTerm):
				return 1
		return 0

	###--- private methods ---###

	def __getTermKeysAndDefaultOrder (self):
		# all vocabulary terms with their pre-assigned sequence
		# numbers and their vocabulary names

		cmd = '''select t._Term_key,
				t.term, 
				t.sequenceNum,
				v.name
			from voc_term t,
				voc_vocab v
			where t._Vocab_key = v._Vocab_key'''

		if VOCABS:
			cmd = '%s and v._Vocab_key in (%s)' % (cmd, VOCABS)

		# retrieve terms from the database

		(cols, rows) = dbAgnostic.execute (cmd)
		logger.debug ('Retrieved %d terms from database' % len(rows))

		# order all terms within a vocabulary by their pre-assigned
		# sequence number from the database, then alphabetically

		keyCol = dbAgnostic.columnNumber (cols, '_Term_key')
		termCol = dbAgnostic.columnNumber (cols, 'term')
		seqCol = dbAgnostic.columnNumber (cols, 'sequenceNum')
		vocabCol = dbAgnostic.columnNumber (cols, 'name')

		termList = []
		for row in rows:
			term = row[termCol]
			seqNum = row[seqCol]
			termKey = row[keyCol]

			if term:
				term = term.lower()

			# special handling to re-order GO DAGs in relation
			# to each other (hack)
			if termKey == 6113:
				seqNum = 3	# biological process
			elif termKey == 120:
				seqNum = 2	# cellular component
			elif termKey == 1098:
				seqNum = 1	# molecular function

			item = (row[vocabCol],
				seqNum,
				term,
				termKey)
			termList.append (item)
		termList.sort()

		i = 0
		for (vocab, seqNum, term, termKey) in termList:
			i = i + 1
			self.defaultOrder[termKey] = i
		logger.debug ('Computed default ordering')

		# map (vocabulary name, term) to the respective term keys

		for row in rows:
			self.termKeys[(row[vocabCol], row[termCol])] = \
				row[keyCol]
		logger.debug ('Mapped vocab/term pairs to term keys')
		return

	def __getDAGs (self):
		# all parent/child relationships for vocab terms (MGIType 13)

		cmd = '''select distinct p._Object_key as parentKey,
				c._Object_key as childKey,
				d._DAG_key as dagKey
			from dag_dag d,
				dag_edge e,
				dag_node p,
				dag_node c
			where d._MGIType_key = 13
				and d._DAG_key = e._DAG_key
				and e._Parent_key = p._Node_key
				and e._Child_key = c._Node_key'''

		if VOCABS:
			cmd = '''%s
				and c._Object_key = t._Term_key
				and t._Vocab_key in (%s)''' % (
					cmd.replace('dag_node c',
						'dag_node c, voc_term t'),
					VOCABS) 

		(cols, rows) = dbAgnostic.execute (cmd)
		logger.debug ('Retrieved %d relationships from database' % \
			len(rows))

		childCol = dbAgnostic.columnNumber (cols, 'childkey')
		parentCol = dbAgnostic.columnNumber (cols, 'parentkey')
		dagCol = dbAgnostic.columnNumber (cols, 'dagkey')

		for row in rows:
			child = row[childCol]
			parent = row[parentCol]

			# we know that a child node is not a root node
			self.notRoots[child] = 1

			# if previously flagged as a root, remove the flag
			if self.roots.has_key(child):
				del self.roots[child]

			# unless we have seen the parent node as a child
			# previously, then it is a root as far as we know now
			if not self.notRoots.has_key(parent):
				self.roots[parent] = row[dagCol]

			# add the child to the list of the parent's children
			if self.children.has_key(parent):
				self.children[parent].append (child)
			else:
				self.children[parent] = [ child ]

		logger.debug ('Found %d roots, %d descendents' % (
			len(self.roots), len(self.notRoots)) )
		return

	def __orderChildren (self):
		# orders the children of each parent node according to their
		# default ordering

		parentKeys = self.children.keys()
		for parent in parentKeys:
			self.children[parent] = self.__orderTermList (
				self.children[parent])

		logger.debug ('Ordered children of %d parents' % \
			len(parentKeys))
		return

	def __orderTermList (self, siblings):
		siblist = []
		for sibling in siblings:
			siblist.append ((self.defaultOrder[sibling], sibling))
		siblist.sort()

		# pull the now-ordered term keys back out of 'siblist'
		return map (lambda y : y[1], siblist)

	def __orderDagRoots (self):
		# provide a custom ordering for the root nodes of our DAGs,
		# since GO has three DAGs that require a certain order among
		# them

		# we will order by DAG key except for those cases where we
		# need a different ordering.  In this case, we put the
		# Molecular Function DAG before the Cellular Component DAG
		dagMap = {
			2 : 1,
			1 : 2,
			}

		rootsByDag = {}

		for (termKey, dagKey) in self.roots.items():

			# re-order DAGs as needed
			if dagMap.has_key(dagKey):
				dagKey = dagMap[dagKey]

			# collect roots for each DAG, in case multiple roots
			# per DAG exist
			if rootsByDag.has_key(dagKey):
				rootsByDag[dagKey].append (termKey)
			else:
				rootsByDag[dagKey] = [ termKey]

		dags = rootsByDag.keys()
		dags.sort()

		# list of roots in proper order, by DAG then term
		roots = []
		for dag in dags:
			# order the roots for this DAG
			rootlist = []
			for root in rootsByDag[dag]:
				rootlist.append ( (self.defaultOrder[root],
					root) )
			rootlist.sort()

			# then pull out those root terms and add them to our
			# list to return
			for (rootOrder, root) in rootlist:
				roots.append(root)
		return roots 

	def __computeFinalOrder (self):
		# order the terms using a DFS algorithm to traverse down the
		# DAGs.  fill in ordering for any non-DAG vocabulary terms as
		# well.  This method fills in self.finalOrder.

		# last-in-first-out structure holding lists of terms to come
		# back and process.  stack[-1] is the most-recent list of
		# siblings that we need to come back and process; once all of
		# those are done, we remove that sublist and begin on the new
		# stack[-1], etc.
		stack = []

		# get the ordered list of roots for the DAGs.  Any terms not
		# reachable from those in 'rootList' are from non-DAG
		# vocabularies; we will come back and order those ones later.
		rootList = self.__orderDagRoots()
		if rootList:
			stack.append (rootList)

		# counter for ordering terms as they are encountered
		seqNum = 0

		while stack:
			# pop the first item off the sublist at the top of the
			# stack
			toDo = stack[-1][0]
			del stack[-1][0]

			# if that leaves an empty sublist at the top of the
			# stack, remove it
			if len(stack[-1]) == 0:
				del stack[-1]

			# if we have not ordered this term, then note its
			# order
			if not self.finalOrder.has_key(toDo):
				seqNum = seqNum + 1
				self.finalOrder[toDo] = seqNum

			# look up the children of this term and add them to
			# the stack
			if self.children.has_key(toDo):
				stack.append (self.children[toDo][:])

		logger.debug ('Ordered %d DAG-based terms' % seqNum)
		dag = seqNum

		# now we need to fill in the ordering for any terms that were
		# not in the DAG-based vocabularies

		termKeys = self.__orderTermList (self.defaultOrder.keys())
		for term in termKeys:
			if not self.finalOrder.has_key(term):
				seqNum = seqNum + 1
				self.finalOrder[term] = seqNum
		logger.debug ('Ordered %d flat vocab terms' % (seqNum - dag))
		return

	def __getTransitiveClosure (self):
		cmd = '''select _AncestorObject_key,
				_DescendentObject_key
			from dag_closure'''

		if VOCABS:
			cmd = '''%s c, voc_term t
				where c._AncestorObject_key = t._Term_key
					and t._Vocab_key in (%s)''' % (cmd,
						VOCABS)

		(cols, rows) = dbAgnostic.execute (cmd)
		logger.debug ('Retrieved %d closure items from database' % \
			len(rows))

		ancestorCol = dbAgnostic.columnNumber (cols,
			'_AncestorObject_key')
		descendentCol = dbAgnostic.columnNumber (cols,
			'_DescendentObject_key')

		for row in rows:
			ancestor = row[ancestorCol]
			descendent = row[descendentCol]

			if not self.ancestors.has_key(descendent):
				self.ancestors[descendent] = {}
			self.ancestors[descendent][ancestor] = 1

		logger.debug ('Cached ancestors for %d terms' % \
			len(self.ancestors))
		return

class VocabSorterAlpha:
	# manages sorting terms two different ways:
	#	1. vocab, term
	#	2. vocab, DAG, term
	# relies on VocabSorter class for some pieces

	def __init__ (self):
		self.vocabTerm = {}	# term key -> sequence num
		self.vocabDagTerm = {}	# term key -> sequence num
		self._loadData()
		return

	###--- private methods ---###

	def _loadData (self):
		# query the database and populate the object's instance
		# variables

		# get vocabulary names to be ordered
		cmd1 = '''select name,
				_Vocab_key as key
			from voc_vocab'''

		if VOCABS:
			cmd1 = '%s where _Vocab_key in (%s)' % (cmd1, VOCABS)

		vocabSeqNum = _prepareOrderingDictionary (cmd1, _vocabNameCmp)

		# get DAG names to be ordered
		cmd2 = '''select name,
				_DAG_key as key
			from dag_dag'''
			
		dagSeqNum = _prepareOrderingDictionary (cmd2, _dagNameCmp)

		# get terms to be ordered

		cmd3 = '''select term as name,
				_Term_key as key
			from voc_term'''

		if VOCABS:
			cmd3 = '%s where _Vocab_key in (%s)' % (cmd3, VOCABS)

		termSeqNum = _prepareOrderingDictionary (cmd3, _smartTermCmp)

		logger.debug (
			'Got sort data for %d vocabs, %d dags, %d terms' % \
			(len(vocabSeqNum), len(dagSeqNum), len(termSeqNum)) )

		# now get all term data so we can order them appropriately

		cmd4 = '''select t._Term_key, t._Vocab_key, d._DAG_key
			from voc_term t
			left outer join dag_node d on (
				t._Term_key = d._Object_key)'''

		if VOCABS:
			cmd4 = '%s where _Vocab_key in (%s)' % (cmd4, VOCABS)

		(cols, rows) = dbAgnostic.execute (cmd4)

		termCol = dbAgnostic.columnNumber (cols, '_Term_key')
		vocabCol = dbAgnostic.columnNumber (cols, '_Vocab_key')
		dagCol = dbAgnostic.columnNumber (cols, '_DAG_key')

		vdtList = []
		vtList = []

		for row in rows:
			termKey = row[termCol]
			vocabKey = row[vocabCol]
			dagKey = row[dagCol]

			termVal = termSeqNum[termKey]
			vocabVal = vocabSeqNum[vocabKey]

			if dagKey == None:
				dagVal = len(dagSeqNum) + 1
			else:
				dagVal = dagSeqNum[dagKey]

			vdtList.append ((vocabVal, dagVal, termVal, termKey))
			vtList.append ((vocabVal, termVal, termKey))

		logger.debug ('Compiled sort data for %d terms' % len(vtList))

		vdtList.sort()
		vtList.sort()

		logger.debug ('Sorted term lists')

		i = 0
		for (v, d, t, key) in vdtList:
			i = i + 1
			self.vocabDagTerm[key] = i

		i = 0
		for (v, t, key) in vtList:
			i = i + 1
			self.vocabTerm[key] = i

		logger.debug ('Assigned seq nums to terms')
		return

	###--- public methods ---###

	def getVocabTermSequenceNum (self, termKey):
		if self.vocabTerm.has_key(termKey):
			return self.vocabTerm[termKey]
		return len(self.vocabTerm) + 1

	def getVocabDagTermSequenceNum (self, termKey):
		if self.vocabDagTerm.has_key(termKey):
			return self.vocabDagTerm[termKey]
		return len(self.vocabDagTerm) + 1

###--- Public Functions ---###

def getTermKey (vocabName, term):
	__initializeVS()
	return VOCAB_SORTER.getTermKey (vocabName, term)

def getSequenceNum (termKey):
	__initializeVS()
	return VOCAB_SORTER.sequenceNum (termKey)

def getSequenceNumByID (accID):
	cmd = '''select _Object_key
		from acc_accession
		where lower(accid) = '%s'
		and _MGIType_key = 13''' % accID.lower()

	(cols, rows) = dbAgnostic.execute(cmd)
	if len(rows) != 1:
		return None
	return getSequenceNum(rows[0][0])

def isChildOf (childTerm, parentTerm):
	__initializeVS()
	return VOCAB_SORTER.isChildOf (childTerm, parentTerm)

def isDescendentOf (descendentTerm, ancestorTerm):
	__initializeVS()
	return VOCAB_SORTER.isDescendentOf (descendentTerm, ancestorTerm)

def getVocabTermSequenceNum (termKey):
	__initializeVSA()
	return VOCAB_SORTER_ALPHA.getVocabTermSequenceNum (termKey)

def getVocabDagTermSequenceNum (termKey):
	__initializeVSA()
	return VOCAB_SORTER_ALPHA.getVocabDagTermSequenceNum (termKey)

def getAssayTypeSequenceNum (assayType):
	# get integer for defined sorting of expression assay types

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

def setVocabs (vocabKeys):
	# call before any other functions to restrict the set of vocabularies
	# pulled in by this module (to optimize for time and memory) for those
	# gatherers which don't need them all

	global VOCABS

	if type(vocabKeys) == type([]):
		VOCABS = ', '.join(map(str, vocabKeys))
	elif type(vocabKeys) == type(1):
		VOCABS = str(vocabKeys)
	elif type(vocabKeys) == type(''):
		VOCABS = vocabKeys
	else:
		raise 'error', 'unknown param in setVocabs: %s' % vocabKeys

	logger.debug('Restricted VocabSorter to only vocabs: %s' % VOCABS)
	return
