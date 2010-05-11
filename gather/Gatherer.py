#!/usr/local/bin/python
# 
# base table gatherer for the Mover suite -- queries Sybase, writes text
#	files.

import os
import tempfile
import sys

import config
import logger
import types
import mysqlUtil
import sybaseUtil

###--- Globals ---###

Error = 'Gatherer.error'	# exception raised by this module

AUTO = 'Gatherer.AUTO'		# special fieldname for auto-incremented field

###--- Classes ---###

class Gatherer:
	# Is: a data gatherer
	# Has: information about queries to execute, fields to retrieve, how
	#	to write data files, etc.
	# Does: queries Sybase for data, collates result sets, and writes
	#	out a single text file of results

	def __init__ (self,
		filenamePrefix, 	# string; prefix of filename to write
		fieldOrder = None, 	# list of strings; ordering of field-
					# ...names in output file
		cmds = None		# list of strings; queries to execute
					# ...against the Sybase database to
					# ...extract data
		):
		self.filenamePrefix = filenamePrefix
		self.cmds = cmds
		self.fieldOrder = fieldOrder

		self.results = []	# list of lists of query results
		self.finalResults = []	# list of collated query results

		# We keep a copy of the 'cmds' as originally submitted in
		# self.baseCmds.  For single-key refreshes, we need to know
		# which fieldname has the key (self.keyField) and what its
		# value should be (self.keyValue).  The latter two are set by
		# self.setKeyRestriction().

		self.keyField = None
		self.keyValue = None
		self.baseCmds = self.cmds[:]

		self.nextAutoKey = None		# integer; auto-increment key
		return

	def setKeyRestriction (self,
		keyField, 	# string; fieldname for restriction by key
				# ...(may use a table prefix if the cmds use
				# ...multiple tables)
		keyValue	# integer; value for that field
		):
		# Purpose: to restrict our data gathering to be a refresh of
		#	data associated with a single database key
		# Returns: nothing
		# Assumes: 'keyField' is a valid database fieldname
		# Modifies: nothing
		# Throws: nothing

		self.keyField = keyField
		self.keyValue = keyValue
		return

	def getKeyClause (self):
		# Purpose: to retrieve the appropriate WHERE clause which
		#	needs to be added to gather only for a single
		#	database key
		# Returns: string (including the ' and ' at the beginning)
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing
		# Notes: This should be defined in subclasses, which know the
		#	supported keyFields and the clauses which would
		#	correspond to them.

		return ''

	def preprocessCommands (self):
		# Purpose: to do any necessary pre-processing of the SQL
		#	queries, such as adding clauses necessary for
		#	retrieving rows for only a single database key
		# Returns: nothing
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		# The commands to be updated initially live in self.baseCmds.
		# For any which include a '%s', then we fill in the keyClause
		# in its place.  Updated commands are placed in self.cmds.

		self.cmds = []
		keyClause = self.getKeyClause()
		if keyClause:
			if cmd.lower().find('where') < 0:
				keyClause = ' where ' + keyClause
			else:
				keyClause = ' and ' + keyClause

		for cmd in self.baseCmds:
			if '%s' in cmd:
				self.cmds.append (cmd % keyClause)
			else:
				self.cmds.append (cmd)
		return

	def gatherByKey (self,
		keyField,	# string; name of field to restrict value
		keyValue	# integer; value desired for 'keyField'
		):
		# Purpose: to do the gathering for a single database key (in
		#	a certain database field)
		# Returns: nothing
		# Assumes: nothing
		# Modifies: queries the database, writes to the file system
		# Throws: propagates any exceptions

		self.setKeyRestriction (keyField, keyValue)
		self.go()
		return

	def gatherAll (self):
		# Purpose: to do all gathering without restriction by key
		# Returns: nothing
		# Assumes: nothing
		# Modifies: queries the database, writes to the file system
		# Throws: propagates any exceptions

		self.setKeyRestriction (None, None)
		self.go()
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
		self.querySybase()
		logger.info ('Finished Sybase queries')
		self.collateResults()
		logger.info ('Built final result set (%d rows)' % \
			len(self.finalResults))
		self.postprocessResults()
		logger.info ('Post-processed result set')
		path, fd = self.createFile()
		logger.info ('Created output file: %s' % path)
		self.writeFile(fd)
		self.closeFile(fd)
		logger.info ('Wrote and closed output file: %s' % path)
		print path
		return

	def querySybase (self):
		# Purpose: to issue the Sybase queries and add the results
		#	to self.results
		# Returns: nothing
		# Assumes: nothing
		# Modifies: queries the database
		# Throws: propagates all exceptions

		if not self.cmds:
			raise Error, 'No commands defined for Gatherer'

		if type(self.cmds) == types.StringType:
			self.cmds = [ self.cmds ]

		i = 0
		for cmd in self.cmds:
			self.results.append (sybaseUtil.sql (cmd, 'auto'))
			logger.debug ('Finished query %d' % i)
			i = i + 1
		return

	def collateResults (self):
		# Purpose: to do any necessary slicing and dicing of
		#	self.results to produce a final set of results to be
		#	written in self.finalResults
		# Returns: nothing
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		self.finalResults = self.results[-1]
		return

	def postprocessResults (self):
		# Purpose: to do any necessary post-processing of the final
		#	results before they are written out to a file
		# Returns: nothing
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		return

	def createFile (self):
		# Purpose: create a uniquely named data file
		# Returns: tuple (string path to filename,
		#	open file descriptor)
		# Assumes: we can write the file
		# Modifies: writes to the file system
		# Throws: propagates all exceptions

		# each file will be named with a specified prefix, a period,
		# then either the database key or 'all', then a dot, then
		# a generated unique identifier, and a '.rpt' suffix

		prefix = self.filenamePrefix + '.'
		if self.keyValue:
			prefix = prefix + str(self.keyValue) + '.'
		else:
			prefix = prefix + 'all.'

		dataDir = config.DATA_DIR

		(fd, path) = tempfile.mkstemp (suffix = '.rpt',
			prefix = prefix, dir = dataDir, text = True)

		return path, fd

	def closeFile (self,
		fd			# file descriptor to which to write
		):
		os.close(fd)
		return

	def writeFile (self,
		fd			# file descriptor to which to write
		):
		# Purpose: to write self.finalResults out to a tab-delimited
		#	file in the file system
		# Returns: None
		# Assumes: 1. the unique field in petal tables is named
		#	uniqueKey; 2. the table we are building is the same
		#	as self.filenamePrefix; 3. we are not running this
		#	script concurrently for two different unique keys
		# Modifies: writes to the file system
		# Throws: propagates all exceptions

		# For petal tables, we need to manage auto-incrementing of a
		# uniqueKey field.  If this is a full gathering, then we can
		# just start over at 1.  If this is a key-based gathering,
		# then we need to start at one more than the current max for
		# the table we're building.

		if self.nextAutoKey == None:
			self.nextAutoKey = 1
			if (self.keyField != None) and (self.keyValue != None):
				if AUTO in self.fieldOrder:
					self.nextAutoKey = 1 + \
						mysqlUtil.findMaxKey (
							self.filenamePrefix)

		for row in self.finalResults:
			columns = []
			for col in self.fieldOrder:
				if col == AUTO:
					columns.append (str(self.nextAutoKey))
					self.nextAutoKey = 1 + self.nextAutoKey
				elif row[col] != None:
					columns.append (str(row[col]))
				else:
					columns.append ('')
			os.write (fd, '\t'.join(columns) + '\n')
		return

class ChunkGatherer (Gatherer):
	# Is: a Gatherer for large data sets -- retrieves and processes data
	#	in chunks, rather than with the whole data set at once

	def go (self):
		# We can just let key-based processing go in the traditional
		# manner.  We only need to process chunks of results if we
		# have no restriction by key.

		if self.keyField != None:
			logger.debug ('Single key-based update')
			Gatherer.go (self)
			return

		logger.debug ('Chunk-based update')

		# find the range of keys we need to support

		minCmd = self.getMinKeyQuery()
		maxCmd = self.getMaxKeyQuery()

		if (minCmd == None) or (maxCmd == None):
			raise Error, 'Required methods not implemented'

		minKey = sybaseUtil.sql(minCmd)[0]['']
		maxKey = sybaseUtil.sql(maxCmd)[0]['']

		if (minKey == None) or (maxKey == None):
			raise Error, 'No data found'

		logger.debug ('Found keys from %d to %d' % (minKey, maxKey))

		# create the output data file

		path, fd = self.createFile()
		logger.info ('Created output file: %s' % path)

		# work through the data chunk by chunk

		lowKey = minKey
		while lowKey <= maxKey:
			highKey = lowKey + config.CHUNK_SIZE
			self.results = []
			self.finalResults = []

			self.preprocessCommandsByChunk (lowKey, highKey)
			self.querySybase()
			self.collateResults()
			self.postprocessResults()
			self.writeFile(fd)
			logger.debug ('Processed and wrote keys %d..%d' % (
				lowKey, highKey - 1))

			lowKey = highKey

		# close the data file and write its path to stdout

		self.closeFile(fd)
		logger.info ('Closed output file: %s' % path)
		print path
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
		# For any which include a '%s', then we fill in the keyClause
		# in its place.  Updated commands are placed in self.cmds.

		self.cmds = []
		keyClause = self.getKeyRangeClause()
		if keyClause:
			keyClause = keyClause % (lowKey, highKey)

		for cmd in self.baseCmds:
			if keyClause:
				if cmd.lower().find('where') < 0:
					kc = ' where ' + keyClause
				else:
					kc = ' and ' + keyClause
			else:
				kc = ''

			if '%s' in cmd:
				self.cmds.append (cmd % kc)
			else:
				self.cmds.append (cmd)
		return

	def getKeyRangeClause (self):
		# Returns: string with two %d arguments, the first being the
		#	lower end of keys to return (inclusive), and the
		#	second being the upper end of keys to return 
		#	(exclusive)

		return

###--- Functions ---###

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

	logger.info (' '.join(sys.argv))
	if len(sys.argv) > 1:
		gatherer.gatherByKey (sys.argv[1], int(sys.argv[2]))
	else:
		gatherer.gatherAll()
	return
