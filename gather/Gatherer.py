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

###--- Globals ---###

Error = 'Gatherer.error'	# exception raised by this module
AUTO = 'Gatherer.AUTO'		# special fieldname for auto-incremented field
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
	if columnName in columns:
		return columns.index(columnName)

	# Postgres returns lowercase fieldnames, so check for that

	c = columnName.lower()
	if c not in columns:
		logger.error ('Column %s (%s) is not in %s' % (columnName, c, 
			', '.join (columns) ) )
		raise Error, 'Unknown column name: %s' % columnName

	return columns.index(c)

def createFile (filename):
	# Purpose: create a uniquely named data file
	# Returns: tuple (string path to filename, open file descriptor)
	# Assumes: we can write the file
	# Modifies: writes to the file system
	# Throws: propagates all exceptions

	# each file will be named with a specified prefix, a period,
	# then a generated unique identifier, and a '.rpt' suffix

	prefix = filename + '.'

	(fd, path) = tempfile.mkstemp (suffix = '.rpt',
		prefix = prefix, dir = config.DATA_DIR, text = True)

	return path, fd

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
		path, fd = createFile (self.filenamePrefix)
		logger.info ('Opened output file: %s' % path)
		self.writeFile(fd)
		os.close(fd)
		logger.info ('Wrote and closed output file: %s' % path)
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

	def writeFile (self,
		fd			# file descriptor to which to write
		):
		# Purpose: to write self.finalResults out to a tab-delimited
		#	file in the file system
		# Returns: None
		# Assumes: 1. the unique field in petal tables is named
		#	uniqueKey; 2. the table we are building is the same
		#	as self.filenamePrefix
		# Modifies: writes to the file system
		# Throws: propagates all exceptions

		# For petal tables, we need to manage auto-incrementing of a
		# uniqueKey field.  Sice we always do a full refresh of the
		# table, then this can just start over at 1.

		if self.nextAutoKey == None:
			self.nextAutoKey = 1

		for row in self.finalResults:
			columns = []
			for col in self.fieldOrder:
				if col == AUTO:
					columns.append (str(self.nextAutoKey))
					self.nextAutoKey = 1 + self.nextAutoKey
				else:
					colNum = columnNumber (
						self.finalColumns, col)
					fieldVal = row[colNum]

					if fieldVal == None:
						columns.append ('') 
					else:
						columns.append (str(fieldVal))

			os.write (fd, '&=&'.join(columns) + '#=#\n')
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

		path, fd = createFile(self.filenamePrefix)
		logger.info ('Created output file: %s' % path)

		# work through the data chunk by chunk

		lowKey = minKey
		while lowKey <= maxKey:
			highKey = lowKey + config.CHUNK_SIZE
			self.results = []
			self.finalResults = []

			self.preprocessCommandsByChunk (lowKey, highKey)
			self.results = executeQueries (self.cmds)
			self.collateResults()
			self.postprocessResults()
			logger.debug ('Post-processed results')
			self.writeFile(fd)
			logger.debug ('Wrote keys %d..%d' % (
				lowKey, highKey - 1))

			lowKey = highKey

		# close the data file and write its path to stdout

		os.close(fd)
		logger.info ('Closed output file: %s' % path)
		print '%s %s' % (path, self.filenamePrefix)
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

		i = 0
		while i < len(self.files):
			filename, fieldOrder, tableName = self.files[i]
			columns, rows = self.output[i]

			path, fd = createFile(filename)
			logger.info ('Opened output file: %s' % path)

			self.writeFile(fieldOrder, columns, rows, fd)
			os.close(fd)
			logger.info('Wrote and closed output file: %s' % path)
			print '%s %s' % (path, tableName)

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

	def writeFile (self,
		fieldOrder,	# list of field names, in order to write out
		columns,	# list of field names, in order of rows
		rows,		# list of lists, each a single row
		fd		# file descriptor to which to write
		):
		# Purpose: to write data from 'rows' out as a tab-delimited
		#	file to 'fd'
		# Returns: None
		# Assumes: nothing
		# Modifies: writes to the file system
		# Throws: propagates all exceptions

		autoKey = 1

#		logger.debug ('fieldOrder: ' + str(fieldOrder))
#		logger.debug ('columns: ' + str(columns))

		for row in rows:
			out = []	# list of field values

			for col in fieldOrder:
				if col == AUTO:
#					logger.debug ('%s == %s' % (col, AUTO))
					out.append (str(autoKey))
					autoKey = 1 + autoKey
				else:
#					logger.debug ('%s != %s' % (col, AUTO))
					colNum = columnNumber (columns, col)
					fieldVal = row[colNum]

					if fieldVal == None:
						out.append ('') 
					else:
						out.append (str(fieldVal))

			os.write (fd, '&=&'.join(out) + '#=#\n')
		return

