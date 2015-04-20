# Module: OutputFile.py
# Purpose: to provide an easy mechanism for opening output BCP files, writing
#	to them, and closing them

import os
import tempfile
import config
import logger
import dbAgnostic
import zlib
import gc

from string import maketrans

###--- Globals ---###

AUTO = 'OutputFile.AUTO'

Error = 'OutputFile.error'
ColumnMismatch = 'Mismatching number of columns: (%d vs %d)'
ClosedFile = 'File was already closed'

# which mode do we use to interpret our output rows to be written?

LIST_MODE = 'list'	    # default - each row is a list
STRING_MODE = 'string'	    # each row is a list encoded as a string
COMPRESS_MODE = 'compress'  # each row is a list encoded as a string, then
			    # ...compressed using zlib
CONTROL_CHARS = ''.join(map(chr, range(0,9) + range(11,13) + range(14,32) + range(127,160)))

###--- Classes ---###

def packRow (mode, row):
	# takes the given 'row' (a list of column values) and encodes it
	# according to the needs of the given 'mode'

	if mode == LIST_MODE:
		return row

	if mode == STRING_MODE:
		return str(row)

	if mode == COMPRESS_MODE:
		return zlib.compress(str(row))

	raise Error, 'Bad mode (%s) in packRow()' % mode

def packRows (mode, rows):
	if mode == LIST_MODE:
		return rows

	newRows = []
	for row in rows:
		newRows.append(packRow(mode, row))
	return newRows

###--- Classes ---###

class OutputFile:
	def __init__ (self, prefix):
		filename = prefix + '.'

		(fd, path) = tempfile.mkstemp (suffix = '.rpt',
			prefix = filename, dir = config.DATA_DIR, text = True)

		self.fd = fd
		self.path = path
		self.rowCount = 0
		self.columnCount = 0
		self.isOpen = True
		self.autoKey = 1
		self.mode = LIST_MODE

		logger.debug ('Opened output file: %s' % self.path)
		return

	def setMode (self, mode):
		if mode not in [ LIST_MODE, STRING_MODE, COMPRESS_MODE ]:
			raise Error, 'Unrecognized mode: %s' % mode

		self.mode = mode
		return

	def unpackRow (self, row):
		if self.mode == LIST_MODE:
			return row

		if self.mode == STRING_MODE:
			return eval(row)

		if self.mode == COMPRESS_MODE:
			return eval(zlib.decompress(row))

		raise Error, 'Bad mode (%s) in unpackRow()' % self.mode

	def close (self):
		os.close(self.fd)
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
			if self.mode != LIST_MODE:
				row = self.unpackRow(row)
				rowCount = rowCount + 1

				if rowCount % 50000 == 0:
					gc.collect()

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
			os.write (self.fd, '\t'.join(cleanedout) + '\n')

		self.rowCount = self.rowCount + len(rows)
		return

###--- Functions ---###

def clean(string):
   string = string.translate(maketrans('', ''), CONTROL_CHARS)
   string = string.replace("\r", " ")
   string = string.replace("\\", "\\\\")
   string = string.replace("\t", "\\\t")
   string = string.replace("\n", "\\\n")
   return string

def createAndWrite (filePrefix, fieldOrder, columns, rows):
	# create a file for filePrefix, write out the data, close it, and
	# return the full file path

	out = OutputFile(filePrefix)
	out.writeToFile (fieldOrder, columns, rows)

	logger.debug ('Wrote %d rows (%d columns) to %s' % (out.getRowCount(),
		out.getColumnCount(), out.getPath()) )
	out.close()

	return out.getPath()
