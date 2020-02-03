# Name: FileReader.py
# Purpose: to provide a convenient mechanism for reading and interacting with data from delimited text files
# Assumptions:
#	1. each line in the file represents a new data row

import os
import dbAgnostic
import logger

###--- constants ---###

TAB = '\t'
LF = '\n'
CR = '\r'

###--- functions ---###

def nullify (rows):
	# go through 'rows' and convert any 'null' strings to be a Python None
	
	converted = 0
	rowCount = len(rows)
	rowNum = 0
	
	while rowNum < rowCount:
		colCount = len(rows[rowNum])
		colNum = 0
		while colNum < colCount:
			if rows[rowNum][colNum] == 'null':
				rows[rowNum][colNum] = None
				converted = converted + 1
			colNum = colNum + 1
		rowNum = rowNum + 1
				
	logger.debug('Converted %d null values to None' % converted)
	return rows

def distinct (rows):
	# go through 'rows' and return only distinct rows (eliminate any duplicate rows)
	
	out = []
	seen = {}
	for row in rows:
		tpl = tuple(row)
		if tpl not in seen:
			seen[tpl] = 1
			out.append(row)
	logger.debug('reduced %d rows to %d distinct' % (len(rows), len(out)))	
	return out
	
###--- classes ---###

class FileReader:
	# Is: a reader for tab-delimited (or other files using a standard delimiter) files, making their
	#	data available in a manner that's similar to the dbAgnostic library does for database queries.
	#	This should ease the path for using a subset of data for initial development (while a data
	#	load is also in development) and then broadening things out to use the full set of data once
	#	loaded.
	# Has: methods for reading a file, getting back all the rows, or filtering it to only get back
	#	some of the columns in each row
	
	def __init__ (self, path, hasHeaderRow = True, delimiter = TAB):
		self.path = path
		self.delimiter = delimiter
		self.hasHeaderRow = hasHeaderRow
		self.columnNames = []
		self.dataRows = []
		self.isPopulated = False
		
		if not os.path.exists(path):
			raise Exception('Path %s does not exist' % path)
		return

	def getPath(self):
		# returns the path to the file
		return self.path
	
	def getColumnNames(self):
		# returns list of column headers for the file
		self._readFile()
		return self.columnNames
		
	def getData(self):
		# returns (list of all column names, list of data rows)
		self._readFile()
		return (self.columnNames, self.dataRows)
		
	def extract(self, columns):
		# returns list of data rows, including only the columns specified (in the order specified)
		# is like getData(), but pulls out a subset of the columns
		
		self._readFile()
		toExtract = []
		for column in columns:
			toExtract.append(dbAgnostic.columnNumber(self.columnNames, column))
			
		out = []
		for row in self.dataRows:
			subset = []
			for column in toExtract:
				subset.append(row[column])
			out.append(subset)

		logger.debug('Extracted %d columns out of %d rows' % (len(toExtract), len(out)))
		return out
		
	def _readFile(self):
		# read and parse the data file
		
		if self.isPopulated:
			return
		
		fp = open(self.path, 'r')
		lines = fp.readlines()
		fp.close()
		logger.debug('Read %d lines from %s' % (len(lines), self.path))

		columnCounts = {}			# maps from column count to number of lines with that count

		for line in lines:
			# skip any blank lines (sometimes happens at end of file)
			if (line.strip() != ''):
				# split the line into columns, strip any trailing whitespace from each
				self.dataRows.append(map(lambda col : col.rstrip(), line.split(self.delimiter)))

				colCount = len(self.dataRows[-1])
				if colCount in columnCounts:
					columnCounts[colCount] = 1 + columnCounts[colCount]
				else:
					columnCounts[colCount] = 1

		if len(columnCounts) > 1:
			raise Exception('Inconsistent number of columns in %s' % self.path)
		
		if self.hasHeaderRow:
			self.columnNames = self.dataRows[0]
			del self.dataRows[0]
		else:
			# if no header row in file, just use column numbers
			for col in range(0, len(self.dataRows[0])):
				self.columnNames.append(col)
		
		self.isPopulated = True
		logger.debug('Parsed %d data rows' % len(self.dataRows))
		return