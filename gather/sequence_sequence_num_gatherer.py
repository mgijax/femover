#!/usr/local/bin/python
# 
# gathers data for the 'sequenceSequenceNum' table in the front-end database

import Gatherer
import logger
import OutputFile
import dbAgnostic

###--- Globals ---###

LENGTH = 'byLength'
TYPE = 'byType'
PROVIDER = 'byProvider'
BATCH_SIZE = 250000

KEY_COL = 0
TYPE_COL = 1
PROVIDER_COL = 2
LENGTH_COL = 3

###--- Functions ---###

def getSequenceBatch (minKey, maxKey):
	# retrieve the needed data for sequences >= minKey and < maxKey.
	# returns as [ (seq key, type key, provider key, length), ... ]
	
	cmd = '''select _Sequence_key, _SequenceType_key, _SequenceProvider_key, length
		from seq_sequence
		where _Sequence_key >= %d
			and _Sequence_key < %d''' % (minKey, maxKey)
			
	columns, rows = dbAgnostic.execute(cmd)
	
	if KEY_COL != Gatherer.columnNumber (columns, '_Sequence_key'):
		raise Exception('Unexpected KEY_COL in results')
	if TYPE_COL !=  Gatherer.columnNumber (columns, '_SequenceType_key'):
		raise Exception('Unexpected TYPE_COL in results')
	if PROVIDER_COL != Gatherer.columnNumber (columns, '_SequenceProvider_key'):
		raise Exception('Unexpected PROVIDER_COL in results')
	if LENGTH_COL != Gatherer.columnNumber (columns, 'length'):
		raise Exception('Unexpected LENGTH_COL in results')

	return rows

###--- Classes ---###

class SequenceSequenceNumGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the sequenceSequenceNum table
	# Has: queries to execute against the source database
	# Does: queries the source database for ordering data for sequences,
	#	collates results, writes tab-delimited text file

	def getTypeOrder (self):
		# returns a dictionary mapping from sequence type key to its ordering number
		
		typeOrder = {}		# typeOrder[type key] = seq num
		i = 0
		KEY_COL = Gatherer.columnNumber (self.results[0][0], '_Term_key')
		for row in self.results[0][1]:
			i = i + 1
			typeOrder[row[KEY_COL]] = i

		logger.debug ('Ordered the %d types' % len(typeOrder))
		return typeOrder

	def getProviderOrder (self):
		# returns a dictionary mapping from provider term key to its ordering number
		# (note that all Genbank providers should be sorted together, rather than separately)

		providerOrder = {}	# providerOrder[provider key] = seq num
		i = 0
		genbankI = None

		KEY_COL = Gatherer.columnNumber (self.results[1][0], '_Term_key')
		termCol = Gatherer.columnNumber (self.results[1][0], 'term')

		for row in self.results[1][1]:
			i = i + 1
			if row[termCol].lower().startswith('genbank'):
				if genbankI == None:
					genbankI = i
				providerOrder[row[KEY_COL]] = genbankI
			else: 
				providerOrder[row[KEY_COL]] = i

		logger.debug ('Ordered the %d providers' % len(providerOrder))
		return providerOrder
	
	def collateResults (self):
		typeOrder = self.getTypeOrder()
		providerOrder = self.getProviderOrder()
		maxLength = self.results[2][1][0][0]
		minKey = self.results[3][1][0][0]
		maxKey = self.results[3][1][0][1]

		# To keep memory requirements down, we're going to use a CachingOutputFile to write results
		# as they're ready.
		
		file = OutputFile.CachingOutputFile(self.filenamePrefix, self.fieldOrder, self.fieldOrder)
		
		# Note that ordering by length is done by taking the maximum sequence length and subtracting
		# the individual sequence's length.  For an ascending sort, we will then get the longest
		# sequences first.
		
		startKey = minKey
		while (startKey <= maxKey):
			stopKey = startKey + BATCH_SIZE
			
			for row in getSequenceBatch(startKey, stopKey):
				if row[LENGTH_COL]:
					r = [ row[KEY_COL],
						maxLength - row[LENGTH_COL],
						typeOrder[row[TYPE_COL]],
						providerOrder[row[PROVIDER_COL]], ]
				else:
					# sort seq with unknown length to the end
					r = [ row[KEY_COL],
						maxLength,
						typeOrder[row[TYPE_COL]],
						providerOrder[row[PROVIDER_COL]], ]

				file.addRow(r)
				
			startKey = stopKey

		logger.debug ('Collated %d sequences' % file.getRowCount())
		file.close()
		return file.getPath()

	def go (self):
		# Purpose: to drive the gathering process from queries through writing the output file
		# Returns: nothing
		# Assumes: nothing
		# Modifies: queries the database, writes to the file system
		# Throws: propagates all exceptions
		# Notes: overrides Gatherer.go() so we don't overwrite our data file (already written
		#	on the fly)

		self.preprocessCommands()
		logger.info ('Pre-processed queries')
		self.results = Gatherer.executeQueries (self.cmds)
		logger.info ('Finished setup queries')
		path = self.collateResults()

		print '%s %s' % (path, self.filenamePrefix)
		return

###--- globals ---###

cmds = [
	# 0. ordered set of sequence type keys
	'''select t._Term_key
	from voc_term t, voc_vocab v
	where t._Vocab_key = v._Vocab_key
		and v.name = 'Sequence Type'
	order by t.sequenceNum''',

	# 1. ordered set of sequence providers
	'''select t._Term_key, t.term
	from voc_term t, voc_vocab v
	where t._Vocab_key = v._Vocab_key
		and v.name = 'Sequence Provider'
	order by t.sequenceNum''',

	# 2. length of longest sequence
	'''select max(length) as maxLength
		from seq_sequence''',
		
	# 3. lowest and highest sequence keys over which we'll need to iterate
	'''select min(_Sequence_key) as minKey, max(_Sequence_key) as maxKey
		from seq_sequence''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Sequence_key', LENGTH, TYPE, PROVIDER, ]

# prefix for the filename of the output file
filenamePrefix = 'sequence_sequence_num'

# global instance of a SequenceSequenceNumGatherer
gatherer = SequenceSequenceNumGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
