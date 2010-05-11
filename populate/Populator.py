#!/usr/local/bin/python
# 
# base table populator for the Mover suite.

import sys
import os
import config
import mysql
import logger

class Populator:
	# Is: a data-loader for the front-end database
	# Has: information about the table to load, the input file, and a
	#	mapping of key names to load-by-key methods
	# Does: reads an input file, refreshes data for an individual key,
	#	refreshes data for the whole table

	def __init__ (self,
		tableInstance, 		# Table object; table to load
		inputFilename = None	# string; path to input file
		):
		# Purpose: constructor
		# Returns: nothing
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		self.table = tableInstance
		self.filename = inputFilename

		# string; cached table name, so we don't have to re-request it
		self.tableName = self.table.getName()

		# maps from key name to method which should be called to
		# refresh the data for that type of key; (populate in
		# subclasses)
		self.keyMethods = {}
		return

	def setKeyMethods (self,
		keyMethods	# dictionary; maps key name to corresponding
				# ...refresh method
		):
		# Purpose: set the keyMethods property for this object
		# Returns: nothing
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		self.keyMethods = keyMethods
		return

	def methodForKey (self,
		fieldname	# string; name of field to refresh by
		):
		# Purpose: determine what method to call to refresh data for
		#	a single key of the given fieldname
		# Return: method pointer (or None, if unknown fieldname)
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		if self.keyMethods.has_key(fieldname):
			return self.keyMethods[fieldname]
		return None

	def deleteByKey (self,
		keyName, 	# string; name of key field
		keyValue	# integer; value for key field
		):
		# Purpose: delete from the table all rows which have the given
		#	'keyValue' for the given 'keyName' field
		# Returns: nothing
		# Assumes: nothing
		# Modifies: see Purpose
		# Throws: propagates any exceptions arising from the deletion

		mysql.execute ('DELETE FROM %s WHERE %s = %s' % (
			self.tableName, keyName, keyValue) )
		logger.info ('Deleted from %s where %s = %s' % (
			self.tableName, keyName, keyValue) )
		return

	def setInputFilename (self,
		inputFilename		# string; path to input filename
		):
		# Purpose: set the input filename for this Populator
		# Returns: nothing
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		self.filename = inputFilename
		return

	def loadFile (self):
		# Purpose: load the contents of the input file into the table
		# Returns: nothing
		# Assumes: nothing
		# Modifies: loads data into the table in the MySQL database
		# Throws: propagates any exceptions resulting from the load
		#	operation

		mysql.execute ("LOAD DATA LOCAL INFILE '%s' INTO TABLE %s" % (
			self.filename, self.tableName))
		mysql.commit()
		logger.info ('Bulk loaded table %s' % self.tableName)
		return

	def go (self):
		# Purpose: do a full drop and reload of the table, with data
		#	from the input file
		# Returns: nothing
		# Assumes: nothing
		# Modifies: recreates and reloads the table in the MySQL db
		# Throws: propagates any exceptions from the drop, create,
		#	load, re-indexing, or re-keying operations

		self.table.dropTable()
		logger.info ('Dropped table %s' % self.tableName)

		self.table.createTable()
		logger.info ('Created table %s' % self.tableName)

		self.loadFile()

		self.table.createIndexes()
		logger.info ('Created indexes on table %s' % self.tableName)
		
		self.table.createKeys()
		logger.info ('Created foreign keys on table %s' % \
			self.tableName)
		
		self.table.createTriggers()
		logger.info ('Created triggers on table %s' % self.tableName)
		return

###--- Functions ---###

def main (
	populator	# Populator object; the instance upon which to work
	):
	# Purpose: to serve as the generic main program for all Populator
	#	subclasses
	# Returns: nothing
	# Assumes: nothing
	# Modifies: alters the populator's table in the MySQL database
	# Throws: propagates any exceptions from the load process

	# add a log entry, noting that the current script was called with its
	# arguments
	logger.info (' '.join(sys.argv))

	populator.setInputFilename (sys.argv[1])

	# Each Populator subclass script must be called with the path to the
	# input file as the first parameter.  If present, a second and third
	# parameter indicate the key name and key value to use in refreshing
	# a subset of the data in the table.

	if len(sys.argv) > 2:
		method = populator.methodForKey (sys.argv[2])
		if method:
			method (int(sys.argv[3]))
	else:
		populator.go()
	return
