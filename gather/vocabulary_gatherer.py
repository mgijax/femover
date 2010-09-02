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
	for child in edges[vocab][term]:
		childDepth = maxDepth(child, vocab, edges)
		deepest = max(deepest, childDepth)

	depthCache[term] = deepest + 1 
	return deepest + 1

###--- Classes ---###

class VocabularyGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the Vocabulary table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for vocabularies,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		# edges[vocab] = {parent : [ child 1, child 2, ... child n ]}
		edges = {}

		# isRoot[vocab][term key] = True/False
		isRoot = {}

		# query 0 - capture the parent/child relationships in memory

		vocCol = Gatherer.columnNumber (self.results[0][0],
			'_Vocab_key')
		pCol = Gatherer.columnNumber (self.results[0][0], 'parentKey')
		cCol = Gatherer.columnNumber (self.results[0][0], 'childKey')

		for row in self.results[0][1]:
			voc = row[vocCol]
			parent = row[pCol]
			child = row[cCol]

			if not edges.has_key(voc):
				edges[voc] = { parent : [ child ] }
			elif not edges[voc].has_key(parent):
				edges[voc][parent] = [ child ]
			else:
				edges[voc][parent].append (child)

			# existing as a child term ensures that this term is
			# not a root term

			if not isRoot.has_key (voc):
				isRoot[voc] = {}

			if isRoot[voc].has_key (child):
				isRoot[voc][child] = False

			# if we have a parent term we have not seen before (as
			# either a parent or a child), then assume it is a
			# root until we discover differently

			if not isRoot[voc].has_key (parent):
				isRoot[voc][parent] = True

		logger.debug ('Read data for %d edges' % \
			len(self.results[0][1]))

		# now examine those edges to find the longest path for each
		# root term to its leaves

		# vocDepth[vocab] = max depth for that vocab
		vocDepth = {}

		vocabs = isRoot.keys()
		for vocab in vocabs:
			vocDepth[vocab] = 0
			terms = isRoot[vocab].keys()
			for term in terms:
				if isRoot[vocab][term] == False:
					continue
				depth = maxDepth (term, vocab, edges)
				vocDepth[vocab] = max(vocDepth[vocab], depth)

		logger.debug ('Found maximum depths for %d vocabs' % \
			len(isRoot))

		# cache for use by postProcess()
		self.vocDepth = vocDepth

		self.finalColumns = self.results[-1][0]
		self.finalResults = self.results[-1][1]
		return

	def postprocessResults (self):
		self.convertFinalResultsToList()

		vocCol = Gatherer.columnNumber (self.finalColumns,
			'_Vocab_key')

		for row in self.finalResults:
			voc = row[vocCol]
			if self.vocDepth.has_key(voc):
				depth = self.vocDepth[voc]
			else:
				depth = 1

			self.addColumn ('maxDepth', depth, row,
				self.finalColumns)
		return

###--- globals ---###

cmds = [
	'''select t._Vocab_key,
			p._Object_key as parentKey,
			c._Object_key as childKey
		from voc_term t,
			dag_node p,
			dag_node c,
			dag_edge e
		where t._Term_key = p._Object_key
			and p._Node_key = e._Parent_key
			and e._Child_key = c._Node_key''',

	'''select v._Vocab_key, v.isSimple, v.name, count(1) as termCount
		from voc_vocab v,
			voc_term t
		where v._Vocab_key = t._Vocab_key
		group by v._Vocab_key, v.isSimple, v.name''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Vocab_key', 'name', 'termCount', 'isSimple', 'maxDepth' ]

# prefix for the filename of the output file
filenamePrefix = 'vocabulary'

# global instance of a VocabularyGatherer
gatherer = VocabularyGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
