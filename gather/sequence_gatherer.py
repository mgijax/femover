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
		organismCol = Gatherer.columnNumber (self.results[0][0],
			'rawOrganism')

		rawLibraries = {}
		organismDict = {}
		for row in self.results[0][1]:
			rawLibraries[row[seqKeyCol]] = row[libraryCol]
			organismDict[row[seqKeyCol]] = row[organismCol]

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

		# pick up the resolved library names

		seqKeyCol = Gatherer.columnNumber (self.results[2][0],
			'_Sequence_key')
		libraryCol = Gatherer.columnNumber (self.results[2][0],
			'library')

		resolvedLibraries = {}
		for row in self.results[2][1]:
			resolvedLibraries[row[seqKeyCol]] = row[libraryCol]

		# merge into a final results set

		self.finalColumns = self.results[-1][0]
		self.finalResults = self.results[-1][1]

		self.convertFinalResultsToList()

		seqKeyCol = Gatherer.columnNumber (self.finalColumns,
			'_Sequence_key')
		organismCol = Gatherer.columnNumber (self.finalColumns,
			'_Organism_key')

		for row in self.finalResults:
			seqKey = row[seqKeyCol]

			# prefer resolved library, then raw library

			library = None
			if resolvedLibraries.has_key(seqKey):
				library = resolvedLibraries[seqKey]

			if (not library) or (library == 'Not Resolved'):
				if rawLibraries.has_key(seqKey):
					if rawLibraries[seqKey] == None:
						library = ''
					else:
						library = rawLibraries[seqKey] + '*'

			self.addColumn ('library', library,
				row, self.finalColumns)

			# prefer resolved organism, then raw organism

			organism = None
			if row[organismCol]:
				organism = Gatherer.resolve (row[organismCol],
					'mgi_organism', '_Organism_key',
					'commonName')

			if (not organism) or (organism == 'Not Resolved'):
				if organismDict.has_key(seqKey):
					organism = organismDict[seqKey] + '*'

			self.addColumn ('organism', organism, row,
				self.finalColumns)

			# primary accession ID and logicalDB

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
		rawLibrary,
		rawOrganism
	from seq_sequence_raw
	where _Sequence_key >= %d and _Sequence_key < %d''',

	'''select _Object_key as _Sequence_key,
		accID,
		_LogicalDB_key
	from acc_accession
	where _MGIType_key = 19
		and preferred = 1
		and _Object_key >= %d and _Object_key < %d''',

	'''select ssa._Sequence_key, ps.name as library
	from seq_source_assoc ssa,
		prb_source ps
	where ssa._Sequence_key >= %d and ssa._Sequence_key < %d
		and ssa._Source_key = ps._Source_key''',

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
	'logicalDB', 'library',
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
