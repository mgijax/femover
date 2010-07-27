#!/usr/local/bin/python
# 
# gathers data for the 'sequenceSequenceNum' table in the front-end database

import Gatherer
import logger

###--- Classes ---###

class SequenceSequenceNumGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the sequenceSequenceNum table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for sequences,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		dict = {}

		typeSeq = {}
		i = 1
		for row in self.results[0]:
			typeSeq[row['_Term_key']] = i
			i = i + 1

		byType = []
		byDescription = []
		byLength = []
		byID = []
		byLocation = []
		for row in self.results[1]:
			sequenceKey = row['_Refs_key']
			d = { '_Refs_key' : sequenceKey,
				'byLength' : 0,
				'byDescription' : 0,
				'byLocation' : 0,
				'bySequenceType' : 0,
				'byPrimaryID' : 0 }
			dict[sequenceKey] = d

			byLength.append ( (row['length'], sequenceKey) )
			byID.append ( (row['accID'], sequenceKey) )
			byDescription.append ( (row['description'].lower(),
				sequenceKey) )
			byType.append ( (typeSeq[row['_SequenceType_key']],
				sequenceKey) )

		for row in self.results[2]:
			byLocation.append ( (row['sequenceNum'],
				row['startCoord'], sequenceKey) )

		logger.debug ('Pulled out data to sort')

		byLength.sort()
		byDescription.sort()
		byType.sort()
		byID.sort()
		byLocation.sort()

		logger.debug ('Sorted data')

		for (lst, field) in [ (byDescription, 'byDescription'),
			(byLength, 'byLength'), (byType, 'byType'),
			(byID, 'byPrimaryID'), (byLocation, 'byLocation') ]:
				i = 1
				for t in lst:
					sequenceKey = t[-1]
					dict[sequenceKey][field] = i
					i = i + 1

		self.finalResults = dict.values() 
		return

	def getMinKeyQuery (self):
		return 'select min(_Sequence_key) from seq_sequence'

	def getMaxKeyQuery (self):
		return 'select max(_Sequence_key) from seq_sequence'

###--- globals ---###

cmds = [
	'''select t.term, t._Term_key
	from VOC_Term t, VOC_Vocab v
	where t._Vocab_key = v._Vocab_key
		and v.name = "Sequence Type"
	order by t.term''',

	'''select m._Sequence_key, m.length, m._SequenceType_key,
		m.description, a.accID
	from SEQ_Sequence m, ACC_Accession a
	where m._Sequence_key = a._Object_key
		and a.preferred = 1
		and a._MGIType_key = 19''',

	'''select m.sequenceNum, m.startCoord, m._Sequence_key
	from SEQ_Sequence m, SEQ_Coord_Cache c, MRK_Chromosome mc
	where m._Sequence_key = c._Sequence_key
		and m._Organism_key = mc._Organism_key
		and c.chromosome = mc.chromosome''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	'_Sequence_key', 'byLength', 'bySequenceType', 'byDescription',
	'byPrimaryID', 'byLocation',
	]

# prefix for the filename of the output file
filenamePrefix = 'sequenceSequenceNum'

# global instance of a SequenceSequenceNumGatherer
gatherer = SequenceSequenceNumGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
