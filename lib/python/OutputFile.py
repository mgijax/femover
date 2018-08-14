# Module: OutputFile.py
# Purpose: to provide an easy mechanism for opening output BCP files, writing
#	to them, and closing them

import os
import tempfile
import string
import config
import logger
import dbAgnostic
import gc


###--- Globals ---###

AUTO = 'OutputFile.AUTO'

Error = 'OutputFile.error'
ColumnMismatch = 'Mismatching number of columns: (%d vs %d)'
ClosedFile = 'File was already closed'

# standard settings for cache size in CachingOutputFile:

SMALL_CACHE = 1000	# only keep 1,000 rows in memory
MEDIUM_CACHE = 10000	# keep 10,000 rows in memory
LARGE_CACHE = 50000	# keep 50,000 rows in memory

###--- Classes ---###

class OutputFile:
	# basic class for handling writing from a gatherer to a file in the
	# file system

	def __init__ (self, prefix, dataDir = config.DATA_DIR, actualName = False):
		filename = prefix + '.'

		self.fd = None		# file descriptor (for temp files generated on every run)
		self.fp = None		# file pointer (for reusable cached files)
		
		if not actualName:
			(self.fd, path) = tempfile.mkstemp (suffix = '.rpt',
				prefix = filename, dir = dataDir, text = True)
		else:
			path = '../data/%srpt' % filename
			self.fp = open(path, 'w')

		self.path = path
		self.rowCount = 0
		self.columnCount = 0
		self.isOpen = True
		self.autoKey = 1

		logger.debug ('Opened output file: %s' % self.path)
		return

	def close (self):
		if self.fd != None:
			os.close(self.fd)
		elif self.fp != None:
			self.fp.close()
			
		self.isOpen = False
		logger.debug ('Closed output file: %s' % self.path)
		return

	def getPath (self):
		return self.path

	def getRowCount (self):
		return self.rowCount

	def getColumnCount (self):
		return self.columnCount

	def writeToFile (self, fieldOrder, columns, rows):

		# ensure that we didn't already close the file

		if not self.isOpen:
			raise Error, ClosedFile

		# if no rows, bail out

		if not rows:
			return

		# check that we have the proper number of columns (that it
		# doesn't differ from any previous data set written to this
		# file)

		if not self.columnCount:
			self.columnCount = len(fieldOrder)
		elif self.columnCount != len(fieldOrder):
			raise Error, ColumnMismatch % (self.columnCount,
				len(fieldOrder))

		# build a new list, parallel to 'fieldOrder', which has the
		# indices of the various fields from 'columns'

		columnNumbers = []
		for col in fieldOrder:
			if col == AUTO:
				columnNumbers.append (AUTO)
			else:
				columnNumbers.append (
					dbAgnostic.columnNumber (columns,
					col) )

		# now go through 'rows' and write each out to the file, with
		# the given field order

		rowCount = self.rowCount

		for row in rows:
			out = []

			for col in columnNumbers:
				if col == AUTO:
					out.append (str(self.autoKey))
					self.autoKey = self.autoKey + 1
				else:
					value = row[col]
					if value == None:
						out.append ('')
					else:
						out.append (str(value))

			cleanedout = [clean(col) for col in out]
			if self.fd != None:
				os.write (self.fd, '\t'.join(cleanedout) + '\n')
			elif self.fp != None:
				self.fp.write('\t'.join(cleanedout) + '\n')

		self.rowCount = self.rowCount + len(rows)
		return

class CachingOutputFile:
	# is a wrapper over an OutputFile, providing a limited-size memory
	# cache and then flushing out to disk whenever that number of rows is
	# reached

	def __init__ (self,
		tableName,	# string; table name used as prefix of output
				# ...filename
		inFieldOrder,	# list of strings; order of fieldnames to
				# ...expect when rows received
		outFieldOrder,	# list of strings; order of fieldnames in 
				# ...which to write out fields to data file
		cacheSize = MEDIUM_CACHE,	# integer; nr of rows to cache
		dataDir = config.DATA_DIR,	# string; path to directory for the file
		actualName = False
		):
		self.outputFile = OutputFile(tableName, dataDir, actualName)
		self.tableName = tableName
		self.inFieldOrder = inFieldOrder
		self.outFieldOrder = outFieldOrder
		self.cacheSize = cacheSize
		self.rowCache = []
		self.rowCount = 0
		self.cachedRowCount = 0
		return

	def __writeCache (self):
		# write out the contents of the cache to the file

		self.rowCount = self.rowCount + self.cachedRowCount
		self.outputFile.writeToFile (self.outFieldOrder,
			self.inFieldOrder, self.rowCache)
		self.rowCache = []
		self.cachedRowCount = 0
		gc.collect()
		return

	def setCacheSize (self,cacheSize):
		# adjust the cache size to be that specified

		self.cacheSize = cacheSize
		return

	def addRow (self, row):
		# add a single row to the file, with fields in order specified
		# by 'inFieldOrder' in constructor

		self.rowCache.append(row)
		self.cachedRowCount = self.cachedRowCount + 1
		if self.cachedRowCount >= self.cacheSize:
			self.__writeCache()
		return

	def addRows (self, rows):
		# add multiple rows to the file, with fields in order
		# specified by 'inFieldOrder' in constructor

		for row in rows:
			self.addRow(row)
		return

	def close (self):
		if self.rowCache:
			self.__writeCache()
		self.outputFile.close()
		return

	def getTableName (self):
		return self.tableName

	def getPath (self):
		return self.outputFile.getPath()

	def getRowCount (self):
		return self.rowCount + self.cachedRowCount

	def getColumnCount (self):
		return len(self.outFieldOrder)

class CachingOutputFileFactory:
	def __init__ (self):
		self.outputFiles = {}
		return

	def createFile (self, prefix, inFieldOrder, outFieldOrder, cacheSize, dataDir = config.DATA_DIR,
			actualName = False):
		# create a new CachingOutputFile with the given parameters
		# and return an integer identifier for it

		f = CachingOutputFile(prefix, inFieldOrder, outFieldOrder, cacheSize, dataDir, actualName)
		num = len(self.outputFiles) + 1
		self.outputFiles[num] = f
		return num

	def setCacheSize (self, num, cacheSize):
		# adjust the cache size of the CachingOutputFile specified by
		# the given 'num'

		self.outputFiles[num].setCacheSize(cacheSize)
		return

	def addRow (self, num, row):
		# add 'row' to the CachingOutputFile specified by 'num'

		self.outputFiles[num].addRow(row)
		return

	def getRowCount (self, num):
		# return the number of rows for the CachingOutputFile
		# specified by 'num'

		return self.outputFiles[num].getRowCount()

	def addRows (self, num, rows):
		# add multiple rows to the CachingOutputFile specified by 'num'

		self.outputFiles[num].addRows(rows)
		return

	def closeAll (self):
		# close all CachingOutputFiles managed by this factory

		for num in self.outputFiles.keys():
			self.outputFiles[num].close()
		return

	def reportAll (self):
		# write to stdout the information we need to have the data
		# files from this factory picked up and loaded into the db

		for num in self.outputFiles.keys():
			print '%s %s' % (self.outputFiles[num].getPath(),
				self.outputFiles[num].getTableName())
		return

###--- Functions ---###

# use a set to boost performance -- clean() function runs in 31% less time
validCharacters = set()
for c in string.printable:
	validCharacters.add(c)
	
def clean(dirtystring):
	dirtystring = filter(lambda x: x in validCharacters, dirtystring)
	
	# make these replacements conditional on whether the string actually contains them or not --
	# runtime of the clean() function is reduced by another 21% beyond the set-based
	# improvement above
	if '\r' in dirtystring:
   		dirtystring = dirtystring.replace("\r", " ")
	if '\\' in dirtystring:
		dirtystring = dirtystring.replace("\\", "\\\\")
	if '\t' in dirtystring:
		dirtystring = dirtystring.replace("\t", "\\\t")
	if '\n' in dirtystring:
		dirtystring = dirtystring.replace("\n", "\\\n")
	
	return dirtystring

def createAndWrite (filePrefix, fieldOrder, columns, rows):
	# create a file for filePrefix, write out the data, close it, and
	# return the full file path

	out = OutputFile(filePrefix)
	out.writeToFile (fieldOrder, columns, rows)

	logger.debug ('Wrote %d rows (%d columns) to %s' % (out.getRowCount(),
		out.getColumnCount(), out.getPath()) )
	out.close()

	return out.getPath()
