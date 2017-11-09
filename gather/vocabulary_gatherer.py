#!/usr/local/bin/python
# 
# gathers data for the 'vocabulary' table in the front-end database

import Gatherer

VocabularyGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the vocabulary table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for vocabularies,
	#	collates results, writes tab-delimited text file

cmds = [
	'''select v._Vocab_key, v.isSimple, v.name, count(1) as termCount
		from voc_vocab v,
			voc_term t
		where v._Vocab_key = t._Vocab_key
		group by v._Vocab_key, v.isSimple, v.name'''
	]

fieldOrder = [ '_Vocab_key', 'name', 'termCount', 'isSimple' ]
filenamePrefix = 'vocabulary'

# global instance of a VocabularyGatherer
gatherer = VocabularyGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
