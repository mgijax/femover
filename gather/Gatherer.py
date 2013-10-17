#!/usr/local/bin/python
# 
# base table gatherer for the Mover suite -- queries Sybase, MySQL, or
#	Postgres as a source database, and writes text files.

import os
import tempfile
import sys

import config
import logger
import types
import dbAgnostic
import OutputFile

###--- Globals ---###

Error = 'Gatherer.error'	# exception raised by this module
AUTO = OutputFile.AUTO		# special fieldname for auto-incremented field
SOURCE_DB = config.SOURCE_TYPE	# either sybase, mysql, or postgres

# cache of terms already looked up
resolveCache = {}		# resolveCache[table][keyField][key] = term

###--- Functions ---###

def resolve (key,		# integer; key value to look up
	table = "voc_term",	# string; table in which to look up key
	keyField = "_Term_key",	# string; field in which to look up key
	stringField = "term"	# string; field with text correponding to key
	):
	# Purpose: to look up the string associated with 'key' in 'table'.
	#	'keyField' specifies the fieldname containing the 'key', while
	#	'stringField' identifies the field with its corresponding
	#	string value
	# Returns: string (or None if no matching key can be found)
	# Assumes: we can query our source database
	# Modifies: adds entries to global 'resolveCache' to cache values as
	#	they are looked up
	# Throws: propagates any exceptions from dbAgnostic.execute()

	global resolveCache

	tableDict = None
	keyDict = None
	fieldDict = None
	term = None

	if resolveCache.has_key(table):
		tableDict = resolveCache[table]
		if tableDict.has_key(keyField):
			keyDict = tableDict[keyField]
			if keyDict.has_key (stringField):
				fieldDict = tableDict[keyField][stringField]
				if fieldDict.has_key(key):
					return fieldDict[key]

	cmd = 'select %s from %s where %s = %d' % (stringField, table,
		keyField, key)

	columns, rows = dbAgnostic.execute(cmd)
	if len(rows) > 0:
		term = rows[0][0]

	if tableDict == None:
		tableDict = {}
		resolveCache[table] = tableDict
		
	if keyDict == None:
		keyDict = {}
		tableDict[keyField] = keyDict

	if fieldDict == None:
		fieldDict = {}
		tableDict[keyField][stringField] = fieldDict

	fieldDict[key] = term
	return term

def columnNumber (columns, columnName):
	return dbAgnostic.columnNumber (columns, columnName)

def executeQueries (cmds):
	# Purpose: to issue the queries and collect the results
	# Returns: list of lists, each with (columns, rows) for a query
	# Assumes: nothing
	# Modifies: queries the database
	# Throws: propagates all exceptions

	if not cmds:
		raise Error, 'No SQL commands given to executeQueries()'

	if type(cmds) == types.StringType:
		cmds = [ cmds ]
	i = 0
	results = []
	for cmd in cmds:
		results.append (dbAgnostic.execute (cmd))
		logger.debug ('Finished query %d' % i)
		i = i + 1
	return results

def main (
	gatherer	# Gatherer object; object to use in gathering data
	):
	# Purpose: to serve as the main program for any Gathering subclass
	# Returns: nothing
	# Assumes: nothing
	# Modifies: queries the database, writes to the file system
	# Throws: propagates all exceptions
	# Notes: Inspects the command-line to determine how to proceed.  If
	#	there were no command-line arguments, then we do a full
	#	gathering operation.  If there are command-line arguments,
	#	then we interpret the first as a keyField and the second as
	#	an integer keyValue; we then do gathering only for that
	#	particular database key.

	if len(sys.argv) > 1:
		raise Error, 'Command-line arguments not supported'
	logger.info ('Begin %s' % sys.argv[0])
	gatherer.go()
	logger.close()
	return

###--- Classes ---###

class Gatherer:
	# Is: a data gatherer
	# Has: information about queries to execute, fields to retrieve, how
	#	to write data files, etc.
	# Does: queries source db for data, collates result sets, and writes
	#	out a single text file of results

	def __init__ (self,
		filenamePrefix, 	# string; prefix of filename to write
		fieldOrder = None, 	# list of strings; ordering of field-
					# ...names in output file
		cmds = None		# list of strings; queries to execute
					# ...against the source database to
					# ...extract data
		):
		self.filenamePrefix = filenamePrefix
		self.cmds = cmds
		self.fieldOrder = fieldOrder

		self.results = []	# list of lists of query results
		self.finalResults = []	# list of collated query results
		self.finalColumns = []	# list of strings (column names) found
					# ...in self.finalResults

		self.nextAutoKey = None		# integer; auto-increment key
		return

	def preprocessCommands (self):
		# Purpose: to do any necessary pre-processing of the SQL
		#	queries
		# Returns: nothing
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		return

	def go (self):
		# Purpose: to drive the gathering process from queries
		#	through writing the output file
		# Returns: nothing
		# Assumes: nothing
		# Modifies: queries the database, writes to the file system
		# Throws: propagates all exceptions

		self.preprocessCommands()
		logger.info ('Pre-processed queries')
		self.results = executeQueries (self.cmds)
		logger.info ('Finished queries of source %s db' % SOURCE_DB)
		self.collateResults()
		logger.info ('Built final result set (%d rows)' % \
			len(self.finalResults))
		self.postprocessResults()
		logger.info ('Post-processed result set')

		path = OutputFile.createAndWrite (self.filenamePrefix,
			self.fieldOrder, self.finalColumns, self.finalResults)

		print '%s %s' % (path, self.filenamePrefix)
		return

	def getTables (self):
		return [ self.filenamePrefix ]

	def collateResults (self):
		# Purpose: to do any necessary slicing and dicing of
		#	self.results to produce a final set of results to be
		#	written in self.finalResults and self.finalColumns
		# Returns: nothing
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		self.finalColumns = self.results[-1][0]
		self.finalResults = self.results[-1][1]
		return

	def convertFinalResultsToList (self):
		self.finalResults = map (list, self.finalResults)
		logger.debug ('Converted %d tuples to lists' % \
			len(self.finalResults))
		return

	def addColumn (self,
		columnName,
		columnValue,
		row,
		columnSet
		):
		if columnName not in columnSet:
			columnSet.append (columnName)
		row.append (columnValue)
		return

	def postprocessResults (self):
		# Purpose: to do any necessary post-processing of the final
		#	results before they are written out to a file
		# Returns: nothing
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		return

class ChunkGatherer (Gatherer):
	# Is: a Gatherer for large data sets -- retrieves and processes data
	#	in chunks, rather than with the whole data set at once
	# Notes: Any query that must be restricted by key must contain two
	#	'%d' fields in it.  (So, for example, loading a slice of rows
	#	into a temp table might have those '%d' fields, and later
	#	queries to process the temp table rows might not.)  The first
	#	'%d' should be for the low end of the range, inclusive: 
	#	(eg- "x >= %d").  The second '%d' should be for the upper end
	#	of the range, exclusive:  (eg- "x < %d")

	def __init__ (self,
		filenamePrefix, 	# string; prefix of filename to write
		fieldOrder = None, 	# list of strings; ordering of field-
					# ...names in output file
		cmds = None		# list of strings; queries to execute
					# ...against the source database to
					# ...extract data
		):
		Gatherer.__init__ (self, filenamePrefix, fieldOrder, cmds)
		self.baseCmds = cmds[:]
		self.chunkSize = config.CHUNK_SIZE
		return

	def setChunkSize (self, newChunkSize):
		self.chunkSize = newChunkSize
		logger.debug ('Set chunk size = %d' % self.chunkSize)
		return

	def go (self):
		# We can just let key-based processing go in the traditional
		# manner.  We only need to process chunks of results if we
		# have no restriction by key.

		logger.debug ('Chunk-based update')

		# find the range of keys we need to support

		minCmd = self.getMinKeyQuery()
		maxCmd = self.getMaxKeyQuery()

		if (minCmd == None) or (maxCmd == None):
			raise Error, 'Required methods not implemented'

		minKey = dbAgnostic.execute(minCmd)[1][0][0]
		maxKey = dbAgnostic.execute(maxCmd)[1][0][0]

		if (minKey == None) or (maxKey == None):
			raise Error, 'No data found'

		logger.debug ('Found keys from %d to %d' % (minKey, maxKey))

		# create the output data file

		out = OutputFile.OutputFile (self.filenamePrefix)

		# work through the data chunk by chunk

		lowKey = minKey
		while lowKey <= maxKey:
			highKey = lowKey + self.chunkSize
			self.results = []
			self.finalResults = []

			self.preprocessCommandsByChunk (lowKey, highKey)
			self.results = executeQueries (self.cmds)
			self.collateResults()
			self.postprocessResults()
			out.writeToFile (self.fieldOrder, self.finalColumns,
				self.finalResults)
			logger.debug ('Wrote keys %d..%d' % (
				lowKey, highKey - 1))

			lowKey = highKey

		# close the data file and write its path to stdout

		out.close()
		logger.debug ('Wrote %d rows to %s' % (out.getRowCount(),
			out.getPath()) )

		print '%s %s' % (out.getPath(), self.filenamePrefix)
		return

	def getMinKeyQuery (self):
		return

	def getMaxKeyQuery (self):
		return

	def preprocessCommandsByChunk (self,
		lowKey,
		highKey
		):
		# The commands to be updated initially live in self.baseCmds.
		# For any which include a '%d', then we fill in the lowKey and
		# highKey in their places.  Updated commands are placed in
		# self.cmds.

		self.cmds = []

		for cmd in self.baseCmds:
			if '%d' in cmd:
				self.cmds.append (cmd % (lowKey, highKey))
			else:
				self.cmds.append (cmd)
		return

class MultiFileGatherer:
	# Is: a Gatherer which handles generating multiple files rather than a
	#	single one.  This is useful for cases where we are generating
	#	unique keys which need to be used for joins to related tables.
	# Has: information about queries to execute, fields to retrieve, how
	#	to write data files, etc.
	# Does: queries source db for data, collates result sets, and writes
	#	out one or more text files of results
	# Notes: This is not a subclass of Gatherer, because it is pretty much
	#	completely re-implemented.  They share a common goal (extract
	#	data from the source database, repackage it, and write data
	#	files), but that is as far as the similarity goes.

	def __init__ (self,
		files,			# list of (filename, field order,
					# ...table name) triplets for output
		cmds = None		# list of strings; queries to execute
					# ...against the source database to
					# ...extract data
		):
		self.files = files
		self.cmds = cmds

		self.results = []	# list of lists of query results (to
					# ...be built from executing 'cmds')

		self.output = []	# list of (list of columns, list of
					# rows), with one per output file, to
					# be filled in by collateResults()
		self.lastWritten = None	# number of last file written so far
		return

	def preprocessCommands (self):
		# Purpose: to do any necessary pre-processing of the SQL
		#	queries
		# Returns: nothing
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		return

	def writeOneFile (self, deleteAfter = True):
		if self.lastWritten == None:
			self.lastWritten = 0
		else:
			self.lastWritten = 1 + self.lastWritten

		if len(self.output) < (self.lastWritten + 1):
			raise Error, \
			'writeOneFile() failed -- data set %d not ready yet' \
			% self.lastWritten

		filename, fieldOrder, tableName = self.files[self.lastWritten]
		columns, rows = self.output[self.lastWritten]

		path = OutputFile.createAndWrite (filename, fieldOrder,
			columns, rows)

		print '%s %s' % (path, tableName)

		if deleteAfter:
			self.output[self.lastWritten] = ([], [])
			logger.info ('Removed data set %d after writing' % \
				self.lastWritten)
		return

	def go (self):
		# Purpose: to drive the gathering process from queries
		#	through writing the output file
		# Returns: nothing
		# Assumes: nothing
		# Modifies: queries the database, writes to the file system;
		#	writes one line to stdout for each file written,
		#	containing the path to the file and the table into
		#	which it should be loaded
		# Throws: propagates all exceptions

		self.preprocessCommands()
		logger.info ('Pre-processed queries')
		if self.cmds:
			self.results = executeQueries (self.cmds)
			logger.info ('Finished queries of source %s db' % \
				SOURCE_DB)
		self.collateResults()
		logger.info ('Built %d result sets' % len(self.output))
		self.postprocessResults()
		logger.info ('Post-processed result sets')

		if len(self.output) != len(self.files):
			raise Error, 'Mismatch: %d files, %d output sets' % (
				len(self.files), len(self.output) )

		if self.lastWritten == None:
			i = 0
		else:
			i = 1 + self.lastWritten

		while i < len(self.files):
			self.writeOneFile()
			i = i + 1
		return

	def getTables (self):
		items = []
		for (filename, fieldOrder, tableName) in self.files:
			items.append (tableName)
		return items

	def collateResults (self):
		# Purpose: to do any necessary slicing and dicing of
		#	self.results to produce a final set of results to be
		#	written in self.output
		# Returns: nothing
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		raise Error, 'Must define collateResults() in subclass'

	def postprocessResults (self):
		# Purpose: to do any necessary post-processing of the final
		#	results before they are written out to files
		# Returns: nothing
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		return
