#!/usr/local/bin/python
# 
# gathers data for the 'term_default_parent' table in the front-end database
#
# This code could eventually be merged in as part of the gatherer for the 'term' table, but we're
# keeping it separate for now while the rules are in flux and until the vocabulary_gatherer is
# refactored (TR12542).
#
# Current vocabularies and their default-parent rules:
#	1. Adult Mouse Anatomy
#		a. prefer is-a relationships over part-of
#		b. use smart alpha to choose first parent

import Gatherer
import logger
import symbolsort

###--- Globals ---###

parentKeyCol = None
parentTermCol = None
edgeLabelCol = None
childKeyCol = None

IS_A = 'is-a'

###--- Function ---###

def amaCompare(a, b):
	# compare tuples based on the AMA rules, which are a three-level sort:
	#	1. sort by child term key
	#	2. then by edge type (is-a relationships first)
	#	3. then smart-alpha by parent term
	
	t = cmp(a[childKeyCol], b[childKeyCol])
	if t:
		return t

	# If one is an is-a relationship and the other is not, prefer the is-a one.  If neither or both are
	# is-a relationships, move on to the next criteria.
	if a[edgeLabelCol] == IS_A:
		if b[edgeLabelCol] != IS_A:
			return -1
	elif b[edgeLabelCol] == IS_A:
		return 1
	
	return symbolsort.nomenCompare(a[parentTermCol], b[parentTermCol])

###--- Classes ---###

class DefaultParentGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the term_default_parent table
	# Has: queries to execute against the source database
	# Does: queries the source database for data needed to identify the default parent term (for display) of
	#	terms from certain vocabularies, collates results, writes tab-delimited text file

	def collateResults(self):
		global parentKeyCol, parentTermCol, edgeLabelCol, childKeyCol

		cols, rows = self.results[0]
		logger.debug('Retrieved %d data rows' % len(rows))
		
		parentKeyCol = Gatherer.columnNumber(cols, 'parent_key')
		parentTermCol = Gatherer.columnNumber(cols, 'parent_term')
		edgeLabelCol = Gatherer.columnNumber(cols, 'label')
		childKeyCol = Gatherer.columnNumber(cols, 'child_key')
		
		rows.sort(amaCompare)
		logger.debug('Sorted %d data rows' % len(rows))
		
		defaultParents = {}		# maps from child key to (default parent key, edge label)
		for row in rows:
			childKey = row[childKeyCol]
			if not childKey in defaultParents:
				defaultParents[childKey] = (row[parentKeyCol], row[edgeLabelCol])

		logger.debug('Assigned default parents for %d children' % len(defaultParents))

		self.finalColumns = [ 'child_key', 'parent_key', 'label' ]
		self.finalResults = []
		
		children = defaultParents.keys()
		children.sort()
		
		for childKey in children:
			self.finalResults.append( (childKey, defaultParents[childKey][0], defaultParents[childKey][1]) )

		logger.debug('Collated result set')
		return
	
###--- globals ---###

cmds = [
	# 0. parent data for each child term key, with a preliminary ordering.
	# includes all parents of the children terms, so we can finalize the ordering in memory and then
	# pick the first parent as the default one.  Includes only AMA at the moment.
	'''select child._Object_key as child_key, l.label,
			parent._Object_key as parent_key, p.term as parent_term
		from voc_vocab v, voc_term t, voc_vocabdag d, dag_node child, dag_edge e,
			dag_node parent, voc_term p, dag_label l
		where v._Vocab_key = t._Vocab_key
			and v.name = 'Adult Mouse Anatomy'
			and v._Vocab_key = d._Vocab_key
			and d._DAG_key = child._DAG_key
			and t._Term_key = child._Object_key
			and child._Node_key = e._Child_key
			and e._Parent_key = parent._Node_key
			and parent._Object_key = p._Term_key
			and e._Label_key = l._Label_key
		order by 1, 2''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'child_key', 'parent_key', 'label', ]

# prefix for the filename of the output file
filenamePrefix = 'term_default_parent'

# global instance of a DefaultParentGatherer
gatherer = DefaultParentGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
