# Module: OutputFile.py
# Purpose: to provide an easy mechanism for opening output BCP files, writing
#	to them, and closing them

import os
import tempfile
import config
import logger
import dbAgnostic

###--- Globals ---###

AUTO = 'OutputFile.AUTO'

Error = 'OutputFile.error'
ColumnMismatch = 'Mismatching number of columns: (%d vs %d)'
ClosedFile = 'File was already closed'

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

		logger.debug ('Opened output file: %s' % self.path)
		return

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

			os.write (self.fd, '&=&'.join(out) + '#=#\n')

		self.rowCount = self.rowCount + len(rows)
		return

###--- Functions ---###

def createAndWrite (filePrefix, fieldOrder, columns, rows):
	# create a file for filePrefix, write out the data, close it, and
	# return the full file path

	out = OutputFile(filePrefix)
	out.writeToFile (fieldOrder, columns, rows)

	logger.debug ('Wrote %d rows (%d columns) to %s' % (out.getRowCount(),
		out.getColumnCount(), out.getPath()) )
	out.close()

	return out.getPath()
