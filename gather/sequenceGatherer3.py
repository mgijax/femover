#!/usr/local/bin/python
# 
# gathers data for the 'sequence' table in the front-end database

import Gatherer
import sybaseUtil

###--- Classes ---###

class SequenceGatherer (Gatherer.ChunkGatherer):
	# Is: a data gatherer for the sequence table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for sequences,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single sequence,
		#	rather than for all sequences

		if self.keyField == 'sequenceKey':
			return 's._Sequence_key = %s' % self.keyValue
		return ''

	def collateResults (self):
		self.finalResults = []

		seqs = {}

		for row in self.results[0]:
			seqs[row['_Sequence_key']] = row

		for row in self.results[1]:
			seqs[row['_Object_key']].update (row)

		for row in self.results[2]:
			seqs[row['_Sequence_key']].update (row)

		seqKeys = seqs.keys()
		seqKeys.sort()

		for key in seqKeys:
			self.finalResults.append (seqs[key])
		return

	def postprocessResults (self):
		# Purpose: we override this method to provide cached key
		#	lookups, to attempt to give a performance boost

		for r in self.finalResults:

			# lookups from VOC_Term

			r['sequenceType'] = sybaseUtil.resolve(
				r['_SequenceType_key'])
			r['quality'] = sybaseUtil.resolve(
				r['_SequenceQuality_key'])
			r['status'] = sybaseUtil.resolve(
				r['_SequenceStatus_key'])
			r['provider'] = sybaseUtil.resolve(
				r['_SequenceProvider_key'])
			r['tissue'] = sybaseUtil.resolve(r['_Tissue_key'])
			r['sex'] = sybaseUtil.resolve(r['_Gender_key'])

			# lookups from other tables

			r['organism'] = sybaseUtil.resolve(r['_Organism_key'],
				'MGI_Organism', '_Organism_key', 'commonName')
			r['logicalDB'] = sybaseUtil.resolve(
				r['_LogicalDB_key'],
				'ACC_LogicalDB', '_LogicalDB_key', 'name')
			r['cellLine'] = sybaseUtil.resolve(
				r['_CellLine_key'],
				'ALL_CellLine', '_CellLine_key', 'cellLine')
			r['strain'] = sybaseUtil.resolve(r['_Strain_key'],
				'PRB_Strain', '_Strain_key', 'strain')
		return

	def getMinKeyQuery (self):
		return 'select min(_Sequence_key) from SEQ_Sequence'

	def getMaxKeyQuery (self):
		return 'select max(_Sequence_key) from SEQ_Sequence'

	def getKeyRangeClause (self):
		# using ACC_Accession rather than SEQ_Sequence because of the
		# presence of a clustered index which should help performance
		# with this range query
		return 'a._Sequence_key >= %d and a._Sequence_key < %d'

###--- globals ---###

cmds = [
	'''select s._Sequence_key,
		s._SequenceType_key,
		s._SequenceQuality_key,
		s._SequenceStatus_key,
		s._SequenceProvider_key,
		s._Organism_key,
		s.length,
		s.description,
		s.version,
		s.division,
		s.virtual as isVirtual,
		s.sequence_date,
		s.seqrecord_date,
		a.rawLibrary,
		a.rawAge as age
	from SEQ_Sequence s,
		SEQ_Sequence_Raw a
	where s._Sequence_key = a._Sequence_key %s''',

	'''select x._Object_key, x.accID, x._LogicalDB_key
	from ACC_Accession x, SEQ_Sequence_Raw a
	where x._MGIType_key = 19
		and x._Object_key = a._Sequence_key
		and x.preferred = 1 %s''',

	'''select ps._Strain_key, ps._Tissue_key, ps._Gender_key,
		ps._CellLine_key, a._Sequence_key
	from SEQ_Source_Assoc a, PRB_Source ps
	where a._Source_key = ps._Source_key %s''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	'_Sequence_key', 'sequenceType', 'quality', 'status', 'provider',
	'organism', 'length', 'description', 'version', 'division',
	'isVirtual', 'sequence_date', 'seqrecord_date', 'accID',
	'logicalDB', 'strain', 'tissue', 'age', 'sex', 'rawLibrary',
	'cellLine',
	]

# prefix for the filename of the output file
filenamePrefix = 'sequence'

# global instance of a sequenceGatherer
gatherer = SequenceGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
