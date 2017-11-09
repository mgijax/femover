#!/usr/local/bin/python
# 
# gathers data for the 'vocabulary' table in the front-end database

import Gatherer
import logger
import gc
import dbAgnostic
import OutputFile

###--- Globals ---###
GO_VOCAB_KEY=4

# indices indicating which data file is which

VOCAB = 0
TERM = 1
TERM_CHILD = 2
TERM_COUNTS = 3
TERM_ANCESTOR = 4
TERM_SIBLING = 6

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

	# Needed to fix a fundamental flaw in this function.  The edge type is
	# supposed to represent the type of edge between the ancestor and
	# its descendent (looking downward).  Up to this point, it has
	# represented the edge type from the ancestor to its ancestor (looking
	# upward).  We just haven't noticed, because we weren't displaying
	# the edge label from this table before building the fewi-based
	# anatomy browser. - jsb, 11/21/2013

	# if we've already found and cached this, just return it

	if pathsUpward.has_key (term):
		return pathsUpward[term]

	# if this term has no ancestors, it is itself a root

	if (not upEdges.has_key(vocab)) or (not upEdges[vocab].has_key(term)):
		# for a root term, it has an empty path upward
		pathsUpward[term] = [ [] ]
		return pathsUpward[term]

	# recursively iterate through this term's ancestors to enumerate all
	# possible paths up to roots

	paths = []
	for (edgeType, ancestor) in upEdges[vocab][term]:
		ancestorPaths = pathsToRoots (ancestor, vocab, upEdges)
		for p in ancestorPaths:
			pNew = p[:]
			pNew.append ( (edgeType, ancestor) )
			paths.append (pNew)

	pathsUpward[term] = paths
	return paths

def resetGlobals():
	global pathsUpward, depthCache

	pathsUpward = {}
	depthCache = {}

	gc.collect()
	return

###--- Classes ---###

class VocabularyGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the vocabulary table and certain term tables
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for vocabularies,
	#	collates results, writes tab-delimited text file

	def resetInstanceVariables (self, vocabKey):
		self.results = []
		self.output = []
		self.lastWritten = None

		self.vocabCmds = []
		for cmd in self.cmds:
			self.vocabCmds.append (cmd % vocabKey)

		gc.collect()
		return

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

	def findTermAncestors (self, upEdges, ids, termByKey, writer):

		# This method is a huge memory hog, especially for the GO
		# vocabulary.  Need to do something to reduce the memory
		# requirements...

		fieldOrder = self.files[TERM_ANCESTOR][1]
		tableName = 'term_ancestor'

		columns = [ 'termKey', 'ancestorTermKey', 'ancestorTerm',
			'ancestorID', 'pathNumber', 'depth', 'edgeLabel' ]
		rows = []

		# pathCache[term] = { parent term key : path number }
		pathCache = {}

		# for each vocab, walk through its terms

		vocabs = upEdges.keys()
		for voc in vocabs:
			terms = upEdges[voc].keys()
			
			logger.debug("processing _vocab_key = %s" % voc)

			# we now write partial results out to 'writer'
			# roughly every 'threshold' terms

			threshold = 100000

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
					pathCache[term][path[-1]] = \
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
							termByKey[ancestor] ]

						if ids.has_key(ancestor):
							row.append(
								ids[ancestor])
						else:
							row.append(None)

						row.append (pathNum)
						row.append (depth)
						row.append (edgeType)
						rows.append (row)

				if len(rows) > threshold:
				    writer.writeToFile (fieldOrder, columns,
					rows)
				    logger.debug('Wrote %d rows for table %s' \
					% (len(rows), tableName))

				    for row in rows:
					del row
				    del rows

				    rows = []
				    gc.collect()

		logger.debug ('Found %d ancestors' % len(rows))
		return pathCache, columns, rows

	def getGOOntologies (self):
		columns, rows = self.results[6]

		tKey = Gatherer.columnNumber (columns, '_Term_key')
		ontologyKey = Gatherer.columnNumber (columns, 'dagShorthand')

		go = {}
		for row in rows:
			go[row[tKey]] = row[ontologyKey]

		return go

	def collateResults (self, fileWriters):

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

		# free up memory from a few large objects before proceeding

		del defs
		del isRoot
		del goOntologies

		gc.collect()

		# step 8 -- term_ancestor table

		pathCache, columns, rows = self.findTermAncestors (upEdges,
			ids, terms, fileWriters[TERM_ANCESTOR])
		self.output.append ( (columns, rows) )

		# free up memory from a few large objects before proceeding

		del upEdges
		gc.collect()

		return

	def getVocabularies (self):
		# get a list of (vocab key, vocab name) tuples, listing each
		# of the vocabularies we need to process

		cmd = '''select _Vocab_key, name
			from voc_vocab
			order by name'''

		(cols, rows) = dbAgnostic.execute (cmd)

		keyCol = dbAgnostic.columnNumber (cols, '_Vocab_key')
		nameCol = dbAgnostic.columnNumber (cols, 'name')

		vocabularies = []

		for row in rows:
			vocabularies.append ( (row[keyCol], row[nameCol]) )

		return vocabularies

	def writeFiles (self, files):
		# write out data to files
		i = 0
		for (filename, fieldOrder, tableName) in self.files:

			writer = files[i]
			columns, rows = self.output[i]

			writer.writeToFile (fieldOrder, columns, rows) 
			logger.debug('Wrote %d rows for table %s' % (
				len(rows), tableName))

			del columns
			del rows
			self.output[i] = [ [], [] ]
			gc.collect()

			i = i + 1
		return

	def go (self):
		# override the standard self.go() method so we can control
		# precisely what gets written to the output files and when.
		# (To lessen memory requirements, we will not write files out
		# in their entirety; we will, instead, process one vocabulary
		# at a time and write them out to the output files piecemeal.)

		logger.debug('Entered self.go() method')

		# instantiate OutputFile objects (one per output file)

		files = []
		for (filename, fieldOrder, tableName) in self.files:
			files.append (OutputFile.OutputFile(filename))
			logger.debug('Opened file for table %s' % tableName)

		# processs standard vocabularies one by one

		for (vocabKey, vocabName) in self.getVocabularies():
			logger.debug('Beginning %s (%d)' % (vocabName,
				vocabKey))

			# reset any global variables
			resetGlobals()

			# reset any instance variables in this Gatherer
			# (also tweak SQL on a per-vocabulary basis)

			self.resetInstanceVariables(vocabKey)

			# execute SQL

			self.results = []
			gc.collect()

			self.results = Gatherer.executeQueries(self.vocabCmds)

			# collate results

			self.collateResults(files)

			# post-process results (if needed)

			self.postprocessResults()
            
			self.writeFiles(files)	
			logger.debug('Finished %s (%d)' % (vocabName,
				vocabKey))


		self.resetInstanceVariables(-1)
	
		#self.writeFiles(files)
		# close output files

		i = 0
		for (filename, fieldOrder, tableName) in self.files:
			files[i].close()
			logger.debug('Closed file for table %s with %d rows' \
				% (tableName, files[i].getRowCount()) )
			print '%s %s' % (files[i].getPath(), tableName)
			i = i + 1

		logger.debug('Exiting self.go() method')
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
			and t._Vocab_key = %d
			and e._Child_key = c._Node_key''',

	# query 1 - IDs, sorted by _LogicalDB_key to bring MGI IDs to the top
	'''select a._Object_key, a.accID, a._LogicalDB_key
		from acc_accession a, voc_term t
		where a._MGIType_key = 13
			and a.preferred = 1
			and a.private = 0
			and a._Object_key = t._Term_key
			and t._Vocab_key = %d
		order by a._LogicalDB_key''',

	# query 2 - vocabularies
	'''select v._Vocab_key, v.isSimple, v.name, count(1) as termCount
		from voc_vocab v,
			voc_term t
		where v._Vocab_key = t._Vocab_key
			and v._Vocab_key = %d
		group by v._Vocab_key, v.isSimple, v.name''',

	# query 3 - term definitions
	'''select x._Term_key, x.sequenceNum, x.note
		from voc_text x, voc_term t
		where x._Term_key = t._Term_key
			and t._Vocab_key = %d
		order by x.sequenceNum''',

	# query 4 - terms
	'''select _Term_key, _Vocab_key, term, sequenceNum, isObsolete,
			abbreviation
		from voc_term
		where _Vocab_key = %d''',

	# query 5 - descendent counts
	'''select c._AncestorObject_key, count(1) as ct
		from DAG_Closure c, voc_term t
		where c._AncestorObject_key = t._Term_key
			and t._Vocab_key = %d
		group by c._AncestorObject_key''',

	# query 6 - shorthand notation for display (instead of vocab name)
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
			and t._Vocab_key = %%d 
			and t._vocab_key = %s'''%GO_VOCAB_KEY,
	]

files = [
	('vocabulary',
		[ '_Vocab_key', 'name', 'termCount', 'isSimple', 'maxDepth' ],
		'vocabulary'),

	('term_counts',
		[ 'termKey', 'pathCount', 'descendentCount', 'childCount',
			'markerCount', 'expressionMarkerCount','creMarkerCount',
			'gxdLitMarkerCount' ],
		'term_counts'),

	# hoping to eliminate this, in favor of term_ancestor_simple
	('term_ancestor',
		[ Gatherer.AUTO, 'termKey', 'ancestorTermKey', 'ancestorTerm',
			'ancestorID', 'pathNumber', 'depth', 'edgeLabel' ],
		'term_ancestor'),
	]

# global instance of a VocabularyGatherer
gatherer = VocabularyGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
