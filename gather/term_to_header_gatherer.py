#!/usr/local/bin/python
# 
# gathers data for the 'term_to_header' table in the front-end database

import Gatherer

GO_VOCAB = 4
MP_VOCAB = 5
EMAPA_VOCAB = 90

###--- Classes ---###

TermToHeaderGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the term_to_header table
	# Has: queries to execute against the source database
	# Does: queries the source database for the mapping from slimgrid
	#	header terms to their descendent terms, collates results,
	#	writes tab-delimited text file

###--- globals ---###

cmds = [
	# 0. union includes all header terms as a self-descendent to include
	# any annotations to the header itself (top of union), plus all the
	# descendent terms which are reachable via the DAG (bottom of union)
	'''select t._Term_key as header_term_key,
		t._Term_key as term_key
	from voc_term t
	where t._Vocab_key in (%d, %d, %d)
		and t.abbreviation is not null
		and t.sequenceNum is not null
	union
	select h._Term_key as header_term_key, t._Term_key as term_key
	from voc_term h, dag_closure dc, voc_term t
	where h._Vocab_key in (%d, %d, %d)
		and h.abbreviation is not null
		and h.sequenceNum is not null
		and h._Term_key = dc._AncestorObject_key
		and dc._DescendentObject_key = t._Term_key''' % (
			GO_VOCAB, MP_VOCAB, EMAPA_VOCAB,
			GO_VOCAB, MP_VOCAB, EMAPA_VOCAB)
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, 'term_key', 'header_term_key'
	]

# prefix for the filename of the output file
filenamePrefix = 'term_to_header'

# global instance of a TermToHeaderGatherer
gatherer = TermToHeaderGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
