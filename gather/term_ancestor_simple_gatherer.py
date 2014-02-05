#!/usr/local/bin/python
# 
# gathers data for the 'term_ancestor_simple' table in the front-end database

import Gatherer
import ADVocab
import logger

###--- Classes ---###

class TermAncestorSimpleGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the term_ancestor_simple table
	# Has: queries to execute against the source database
	# Does: queries the source database for ancestors of vocabulary terms,
	#	collates results, writes tab-delimited text file

	def collateResults (self):

		# first, handle the traditional DAG-organized vocabularies
		# from query 0.

		cols, rows = self.results[0]

		keyCol = Gatherer.columnNumber (cols, 'termKey')
		termCol = Gatherer.columnNumber (cols, 'ancestorTerm')
		idCol = Gatherer.columnNumber (cols, 'accID')

		ancestors = {}		# term key -> { ancestor ID : 1 }
		self.finalResults = []
		self.finalColumns = ['termKey', 'ancestorTerm', 'ancestorID']

		logger.debug ('Got %d ancestor rows' % len(rows))

		for row in rows:
			termKey = row[keyCol]
			accID = row[idCol]

			if ancestors.has_key(termKey):

				# if we've already seen this ancestors for
				# this term, then skip it

				if ancestors[termKey].has_key(accID):
					continue

				# otherwise, add this ancestor to the set of
				# those seen

				ancestors[termKey][accID] = 1
			else:
				# this is the first ancestor for this term
				ancestors[termKey] = { accID : 1 }

			self.finalResults.append ( [ termKey, row[termCol],
				accID ] )

#			logger.debug ('Collapsed to %d unique ancestors' % \
#				len(self.finalResults))

		# next, handle the ancestors from the anatomical dictionary

		# clear this to allow memory to be reclaimed
		ancestors = {}	

		# specify the columns and their order for the ancestors, then
		# we can simply append it to the existing list of rows

		adRows = ADVocab.getTermAncestorRows ( [ 'termKey',
			'ancestorTerm', 'ancestorID' ] )

		# Because the AD is tree-structured (rather than a full DAG),
		# there should only be one path to each node, and thus no
		# duplication of ancestors for a single term.  As a result,
		# we do not need to worry about omitting duplicates.
			
		self.finalResults = self.finalResults + adRows

		logger.debug ('Added %d ancestors for AD' % len(adRows))
		return

###--- globals ---###

cmds = [
	# 0. ancestors for traditional vocabulary terms (not the AD);
	#	could to a 'distinct' here, but we'll do it in code to save
	#	load on the database, and hopefully get a better response time
	'''select dc._DescendentObject_key as termKey,
		t.term as ancestorTerm,
		a.accID
	from DAG_DAG dd,
		DAG_Closure dc,
		VOC_Term t,
		ACC_Accession a,
		VOC_Vocab v
	where dd._DAG_key = dc._DAG_key
		and dd._MGIType_key = 13
		and dc._AncestorObject_key = t._Term_key
		and t._Vocab_key = v._Vocab_key
		and dc._AncestorObject_key = a._Object_key
		and v._LogicalDB_key = a._LogicalDB_key
		and a._MGIType_key = 13
		and a.private = 0
		and a.preferred = 1''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'termKey', 'ancestorTerm', 'ancestorID', ]

# prefix for the filename of the output file
filenamePrefix = 'term_ancestor_simple'

# global instance of a TermAncestorSimpleGatherer
gatherer = TermAncestorSimpleGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
