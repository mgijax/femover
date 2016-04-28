#!/usr/local/bin/python
# 
# gathers data for the 'term_to_term' table in the front-end database.
# This table holds relationships from one vocabulary term to another.

import Gatherer
import logger
import gc

###--- Globals ---###

###--- Functions ---###

###--- Classes ---###

i = 0		# global sequence num

class TermToTermGatherer (Gatherer.CachingMultiFileGatherer):
	# Is: a data gatherer for the term_to_term table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for relationships
	#	among terms, collates results, writes tab-delimited text file
	# Note: This doesn't have to be a multi-file gatherer, as it only
	#	produces a single file, but it's convenient for the automatic
	#	management of how many output rows to keep in memory.

	def collateResults (self):
		# slice and dice the query results to produce our set of
		# final results

		cols, rows = self.results[0]

		term1 = Gatherer.columnNumber(cols, 'termKey1')
		term2 = Gatherer.columnNumber(cols, 'termKey2')
		relType = Gatherer.columnNumber(cols, 'relationship_type')

		for row in rows:
			self.addRow('term_to_term',
				[ row[term1], row[term2], row[relType] ])

		logger.debug ('Collected %d term relationships' % len(rows))
		return

###--- globals ---###

cmds = [
	# 0. OMIM terms to their HPO terms (top of union), plus
	#	MP headers to HPO terms (bottom of union)
	'''select a._Object_key as termKey1, a._Term_key as termKey2,
		'OMIM to HPO' as relationship_type
	from voc_annot a
	where a._AnnotType_key = 1018
	union
	select _Object_key_1, _Object_key_2, 'MP header to HPO high-level' 
	from mgi_relationship
	where _Category_key = 1005
	''',
	]

# order of fields (from the query results) to be written to the
# output file
files = [ ('term_to_term',
		[ 'termKey1', 'termKey2', 'relationship_type' ],
		[ Gatherer.AUTO, 'termKey1', 'termKey2', 'relationship_type' ]),
	]

# global instance of a markerIDGatherer
gatherer = TermToTermGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
