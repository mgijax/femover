#!/usr/local/bin/python
# 
# gathers data for the 'sequence' table in the front-end database

import config
import Gatherer

###--- Classes ---###

class SequenceGatherer (Gatherer.ChunkGatherer):
	# Is: a data gatherer for the sequence table
	# Has: queries to execute against the source database
	# Does: queries source database for primary data for sequences,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		# build a dictionary of library info

		seqKeyCol = Gatherer.columnNumber (self.results[0][0],
			'_Sequence_key')
		libraryCol = Gatherer.columnNumber (self.results[0][0],
			'rawLibrary')

		libraryDict = {}
		for row in self.results[0][1]:
			libraryDict[row[seqKeyCol]] = row[libraryCol]

		# build a dictionary of accession info

		seqKeyCol = Gatherer.columnNumber (self.results[1][0],
			'_Sequence_key')
		accCol = Gatherer.columnNumber (self.results[1][0],
			'accID')
		ldbCol = Gatherer.columnNumber (self.results[1][0],
			'_LogicalDB_key')

		accDict = {}
		for row in self.results[1][1]:
			ldb = Gatherer.resolve (row[ldbCol], 'acc_logicaldb',
				'_LogicalDB_key', 'name')
			accDict[row[seqKeyCol]] = (row[accCol], ldb)

		# merge into a final results set

		self.finalColumns = self.results[-1][0]
		self.finalResults = self.results[-1][1]

		self.convertFinalResultsToList()

		seqKeyCol = Gatherer.columnNumber (self.finalColumns,
			'_Sequence_key')

		for row in self.finalResults:
			seqKey = row[seqKeyCol]

			if libraryDict.has_key(seqKey):
				library = libraryDict[seqKey]
			else:
				library = None

			self.addColumn ('rawLibrary', library,
				row, self.finalColumns)

			if accDict.has_key(seqKey):
				accID, ldb = accDict[seqKey]
			else:
				accID = None
				ldb = None

			self.addColumn ('accID', accID, row, 
				self.finalColumns)
			self.addColumn ('logicalDB', ldb, row,
				self.finalColumns)
		return

	def postprocessResults (self):
		# Purpose: we override this method to provide cached key
		#	lookups, to attempt to give a performance boost

		seqTypeCol = Gatherer.columnNumber (self.finalColumns,
			'_SequenceType_key')
		qualityCol = Gatherer.columnNumber (self.finalColumns,
			'_SequenceQuality_key')
		statusCol = Gatherer.columnNumber (self.finalColumns,
			'_SequenceStatus_key')
		providerCol = Gatherer.columnNumber (self.finalColumns,
			'_SequenceProvider_key')
		organismCol = Gatherer.columnNumber (self.finalColumns,
			'_Organism_key')

		for r in self.finalResults:

			# lookups from voc_term

			self.addColumn ('sequenceType', Gatherer.resolve(
				r[seqTypeCol]), r, self.finalColumns)
			self.addColumn ('quality', Gatherer.resolve(
				r[qualityCol]), r, self.finalColumns)
			self.addColumn ('status', Gatherer.resolve(
				r[statusCol]), r, self.finalColumns)
			self.addColumn ('provider', Gatherer.resolve(
				r[providerCol]), r, self.finalColumns)

			# lookups from other tables

			self.addColumn ('organism', Gatherer.resolve (
				r[organismCol], 'mgi_organism',
				'_Organism_key', 'commonName'),
				r, self.finalColumns)
		return

	def getMinKeyQuery (self):
		return 'select min(_Sequence_key) from seq_sequence'

	def getMaxKeyQuery (self):
		return 'select max(_Sequence_key) from seq_sequence'

###--- globals ---###

if config.SOURCE_TYPE == 'sybase':
	sd = 'convert(varchar(10), s.sequence_date, 101)'
	srd = 'convert(varchar(10), s.seqrecord_date, 101)'
elif config.SOURCE_TYPE == 'mysql':
	sd = "date_format(s.sequence_date, '%%m/%%d/%%Y')"
	srd = "date_format(s.seqrecord_date, '%%m/%%d/%%Y')"
elif config.SOURCE_TYPE == 'postgres':
	sd = "to_char(s.sequence_date, 'MM/DD/YYYY')"
	srd = "to_char(s.seqrecord_date, 'MM/DD/YYYY')"

cmds = [
	# in an attempt to improve efficiency, we just do multiple one-table
	# queries and will then join the results in code

	'''select _Sequence_key,
		rawLibrary
	from seq_sequence_raw
	where _Sequence_key >= %d and _Sequence_key < %d''',

	'''select _Object_key as _Sequence_key,
		accID,
		_LogicalDB_key
	from acc_accession
	where _MGIType_key = 19
		and preferred = 1
		and _Object_key >= %d and _Object_key < %d''',

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
		%s as sequenceDate,
		%s as seqrecordDate
	from seq_sequence s
	where s._Sequence_key >= %s and s._Sequence_key < %s''' % (
			sd, srd, '%d', '%d'),
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	'_Sequence_key', 'sequenceType', 'quality', 'status', 'provider',
	'organism', 'length', 'description', 'version', 'division',
	'isVirtual', 'sequenceDate', 'seqrecordDate', 'accID',
	'logicalDB', 'rawLibrary',
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
