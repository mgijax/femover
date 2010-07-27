#!/usr/local/bin/python
# 
# gathers data for the 'sequence' table in the front-end database

import Gatherer

###--- Classes ---###

class SequenceGatherer (Gatherer.ChunkGatherer):
	# Is: a data gatherer for the sequence table
	# Has: queries to execute against the source database
	# Does: queries source database for primary data for sequences,
	#	collates results, writes tab-delimited text file

	def postprocessResults (self):
		# Purpose: we override this method to provide cached key
		#	lookups, to attempt to give a performance boost

		self.convertFinalResultsToList()

		seqTypeCol = Gatherer.columnNumber (self.finalColumns,
			'_SequenceType_key')
		qualityCol = Gatherer.columnNumber (self.finalColumns,
			'_SequenceQuality_key')
		statusCol = Gatherer.columnNumber (self.finalColumns,
			'_SequenceStatus_key')
		providerCol = Gatherer.columnNumber (self.finalColumns,
			'_SequenceProvider_key')
		tissueCol = Gatherer.columnNumber (self.finalColumns,
			'_Tissue_key')
		sexCol = Gatherer.columnNumber (self.finalColumns,
			'_Gender_key')
		organismCol = Gatherer.columnNumber (self.finalColumns,
			'_Organism_key')
		ldbCol = Gatherer.columnNumber (self.finalColumns,
			'_LogicalDB_key')
		cellCol = Gatherer.columnNumber (self.finalColumns,
			'_CellLine_key')
		strainCol = Gatherer.columnNumber (self.finalColumns,
			'_Strain_key')

		for r in self.finalResults:

			# lookups from VOC_Term

			self.addColumn ('sequenceType', Gatherer.resolve(
				r[seqTypeCol]), r, self.finalColumns)
			self.addColumn ('quality', Gatherer.resolve(
				r[qualityCol]), r, self.finalColumns)
			self.addColumn ('status', Gatherer.resolve(
				r[statusCol]), r, self.finalColumns)
			self.addColumn ('provider', Gatherer.resolve(
				r[providerCol]), r, self.finalColumns)
			self.addColumn ('tissue', Gatherer.resolve(
				r[tissueCol]), r, self.finalColumns)
			self.addColumn ('sex', Gatherer.resolve(
				r[sexCol]), r, self.finalColumns)

			# lookups from other tables

			self.addColumn ('organism', Gatherer.resolve (
				r[organismCol], 'mgi_organism',
				'_Organism_key', 'commonName'),
				r, self.finalColumns)
			self.addColumn ('logicalDB', Gatherer.resolve(
				r[ldbCol], 'acc_logicaldb', '_LogicalDB_key',
				'name'), r, self.finalColumns)
			self.addColumn ('cellLine', Gatherer.resolve(
				r[cellCol], 'all_cellline', '_CellLine_key',
				'cellLine'), r, self.finalColumns)
			self.addColumn ('strain', Gatherer.resolve(
				r[strainCol], 'prb_strain', '_Strain_key',
				'strain'), r, self.finalColumns)
		return

	def getMinKeyQuery (self):
		return 'select min(_Sequence_key) from seq_sequence'

	def getMaxKeyQuery (self):
		return 'select max(_Sequence_key) from seq_sequence'

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
		a.accID,
		a._LogicalDB_key,
		ps._Strain_key,
		ps._Tissue_key,
		ps._Gender_key,
		ssr.rawLibrary,
		ps.age,
		ps._CellLine_key
	from seq_sequence s,
		acc_accession a,
		seq_source_assoc ssa,
		prb_source ps,
		seq_sequence_raw ssr
	where s._Sequence_key = ssa._Sequence_key
		and s._Sequence_key >= %d and s._Sequence_key < %d
		and ssa._Source_key = ps._Source_key
		and s._Sequence_key = a._Object_key
		and a._MGIType_key = 19
		and a.preferred = 1
		and s._Sequence_key = ssr._Sequence_key''',
	]

# order of fields (from the query results) to be written to the
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
