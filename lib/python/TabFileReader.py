# Name: TabFileReader.py
# Purpose: provides a wrapper for reading tab-delimited files, often used for
#	loading test data while early in development (before real data gets
#	loaded on the back-end side).
# Assumes: tab-delimited files were exported from Excel (uses the standard
#	'excel-tab' dialect of the csv library
# Notes: This is based upon the standard csv module, but provides a more
#	Java-like iterator.  The next() method returns None when it runs out
#	of rows, rather than throwing an Exception, so any of these methods of
#	use should work fine:
#
#	Given:
#		x = TabFileReader('/path/to/my/file')
#	Either:
#		while x.has_next():
#			doSomethingWith(x.next())
#	Or:
#		y = x.next()
#		while y:
#			doSomethingWith(y)
#			y = x.next()
#	Or:
#		for y in x.allRows():
#			doSomethingWith(y)

import csv
import os

def throw(exception):
	raise Exception('TabFileReader.error : %s' % exception)

class TabFileReader:
	# Is: a reader for Excel-generated tab-delimited files
	# Has: a data file containing zero or more lines
	# Does: opens a data file and serves up lines as dictionaries

	def __init__ (self,
		filepath,			# string; path to data file
		skipLines=None,			# int; number of lines to skip
		fieldnames=None			# list of strings; each is...
						# ...a column name
		):
		# constructor; If 'skipLines' is specified, then we skip the
		# first 'skipLines' lines in the input file.  If 'fieldnames'
		# is specified, then we use those names as the keys in the row
		# dictionaries.  If 'fieldnames' is not specified, then we
		# will use the first non-skipped row as our column headers.

		# member variables

		self.filename = filepath	# full path to input file
		self.done = False		# are we done with this file?
		self.rawfile = None		# raw input file pointer
		self.csvfile = None		# will be a csv.DictReader
		self.cachedLine = None		# cache one data line
		self.skipLines = skipLines	# number of lines to skip
		self.fieldnames = fieldnames	# column names

		# check that the input file exists

		if not os.path.exists(self.filename):
			self.done = True
			throw('Missing data file: %s' % self.filename)

		# open the input file

		try:
			self.rawfile = open(self.filename, 'r')
		except:
			self.done = True
			throw('Cannot open data file: %s' % self.filename)

		# skip over any unneeded lines

		if self.skipLines:
			try:
				for i in range(0, self.skipLines):
					self.rawfile.readline()
			except:
				self.done = True
				throw(
				    'Cannot advance data file (%s) %d lines' \
					% (self.filename, self.skipLines) )

		# use the standard csv library from here on...

		try:
			if self.fieldnames:
				self.csvfile = csv.DictReader(self.rawfile,
					dialect='excel-tab',
					fieldnames=self.fieldnames)
			else:
				self.csvfile = csv.DictReader(self.rawfile,
					dialect='excel-tab')
		except:
			self.done = True
			throw('Cannot use data file as csv file: %s' \
				% self.filename)
		return

	def has_next(self):
		# returns boolean; is there another row to return?

		# If we already have a line cached, then we know it's there.

		if self.cachedLine:
			return True

		# Or, if we've already hit the end of the file, no more lines.

		if self.done:
			return False

		# Try to cache another line and report accordingly.

		self.cachedLine = self.next()

		if self.cachedLine == None:
			return False
		return True

	def next(self):
		# returns the next data row as a dictionary

		# if we have a row cached, then return that one

		if self.cachedLine:
			d = self.cachedLine
			self.cachedLine = None
			return d

		# if we've already hit the end of the file, then we're done

		elif self.done:
			return None

		# otherwise, try to get a new row from the file and return
		# that one

		try:
			return self.csvfile.next()
		except:
			# if we hit the end of the file, then we're done
			self.done = True

		return None

	def allRows(self):
		# return all rows from this file, as a list of dictionaries

		items = []
		while self.has_next():
			items.append(self.next())
		return items
