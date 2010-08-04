#!/usr/local/bin/python
# 
# gathers data for the 'sequenceSequenceNum' table in the front-end database

import Gatherer
import logger

###--- Globals ---###

LENGTH = 'byLength'
TYPE = 'byType'
PROVIDER = 'byProvider'

###--- Classes ---###

class SequenceSequenceNumGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the sequenceSequenceNum table
	# Has: queries to execute against the source database
	# Does: queries the source database for ordering data for sequences,
	#	collates results, writes tab-delimited text file

	def collateResults (self):

		# compute and cache the ordering for sequence types

		typeOrder = {}		# typeOrder[type key] = seq num
		i = 0
		keyCol = Gatherer.columnNumber (self.results[0][0],
			'_Term_key')
		for row in self.results[0][1]:
			i = i + 1
			typeOrder[row[keyCol]] = i

		logger.debug ('Ordered the %d types' % len(typeOrder))

		# compute and cache the ordering for sequence providers

		providerOrder = {}	# providerOrder[provider key] =seq num
		i = 0
		keyCol = Gatherer.columnNumber (self.results[1][0],
			'_Term_key')
		for row in self.results[1][1]:
			i = i + 1
			providerOrder[row[keyCol]] = i

		logger.debug ('Ordered the %d providers' % len(providerOrder))

		# ordering by length is done by taking the maximum sequence
		# length and subtracting the individual sequence's length.
		# for an ascending sort, we will then get the longest
		# sequences first

		maxLength = self.results[2][1][0][0]

		columns = self.results[3][0]
		keyCol = Gatherer.columnNumber (columns, '_Sequence_key')
		typeCol = Gatherer.columnNumber (columns, '_SequenceType_key')
		provCol = Gatherer.columnNumber (columns,
			'_SequenceProvider_key')
		lenCol = Gatherer.columnNumber (columns, 'length')

		self.finalColumns = [ '_Sequence_key', LENGTH, TYPE, PROVIDER]
		self.finalResults = []

		for row in self.results[3][1]:
			if row[lenCol]:
				r = [ row[keyCol],
					maxLength - row[lenCol],
					typeOrder[row[typeCol]],
					providerOrder[row[provCol]], ]
			else:
				# sort seq with unknown length to the end
				r = [ row[keyCol],
					maxLength,
					typeOrder[row[typeCol]],
					providerOrder[row[provCol]], ]

			self.finalResults.append (r)

		logger.debug ('Ordered %d by length' % len(self.finalResults))
		return

###--- globals ---###

cmds = [
	'''select t._Term_key
	from voc_term t, voc_vocab v
	where t._Vocab_key = v._Vocab_key
		and v.name = 'Sequence Type'
	order by t.sequenceNum''',

	'''select t._Term_key
	from voc_term t, voc_vocab v
	where t._Vocab_key = v._Vocab_key
		and v.name = 'Sequence Provider'
	order by t.sequenceNum''',

	'''select max(length) as maxLength
		from seq_sequence''',

	'''select _Sequence_key, _SequenceType_key, _SequenceProvider_key,
			length
		from seq_sequence''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Sequence_key', LENGTH, TYPE, PROVIDER, ]

# prefix for the filename of the output file
filenamePrefix = 'sequenceSequenceNum'

# global instance of a SequenceSequenceNumGatherer
gatherer = SequenceSequenceNumGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
