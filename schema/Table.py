#!/usr/local/bin/python

import sys
import os
import getopt
import config
import mysql

###--- Globals ---###

# Usage statement for all Table scripts, when executed from the command-line
USAGE = '''Usage: %s [--dT|--cT|--di|--ci|--dk|--ck|--dt|--ct]
    Options:
	--dT : drop table (with its indexes, keys, and triggers)
	--cT : create table
	--di : drop indexes
	--ci : create indexes
	--dk : drop foreign keys
	--ck : create foreign keys
	--dt : drop triggers
	--ct : create triggers
    Notes:
    	1. At least one command-line option must be specified.
	2. If one or more operations fail, this script will exit with a
	   non-zero exit code.  The state of the database at that point
	   is not guaranteed to be correct.
	3. In the case of multiple command-line options, they are executed
	   in a certain order (left to right):
		--dt, --dk, --di, --dT, --cT, --ci, --ck, --ct
''' % os.path.basename(sys.argv[0])

# exception raised if things go wrong
Error = 'Table.Error'

###--- Functions ---###

def bailout (
	message, 		# string; error message to write out to user
	showUsage = False	# boolean; show the Usage string or not?
	):
	# Purpose: exit the script with an error message and a non-zero return
	#	code
	# Returns: does not return
	# Assumes: nothing
	# Modifies: writes to stderr
	# Throws: SystemExit to exit the script

	if showUsage:
		sys.stderr.write (USAGE + '\n')
	if message:
		sys.stderr.write ('Error: %s\n' % message)
	sys.exit(1)

def main (
	tableInstance		# Table object upon which to operate
	):
	# Purpose: generic main program for all Table subclasses
	# Returns: nothing
	# Assumes: nothing
	# Modifies: table triggers, keys, indexes, and definition in the MySQL
	#	database
	# Throws: propagates any exceptions raised

	# at least one command-line argument must be specified

	if len(sys.argv) == 1:
		bailout ('No options were specified', True)

	# command-line flags specified by the user
	userFlags = sys.argv[1:]

	# order in which we must execute the user's specified flags
	flagOrder = '--dt --dk --di --dT --cT --ci --ck --ct'.split()

	# check that all user-specified flags are recognized
	for flag in userFlags:
		if not flag in flagOrder:
			bailout ('Unknown option (%s)' % flag, True)

	# execute the user's flags in the proper order
	for flag in flagOrder:
		if flag in userFlags:
			if flag == '--dt':
				tableInstance.dropTriggers()
			elif flag == '--dk':
				tableInstance.dropKeys()
			elif flag == '--di':
				tableInstance.dropIndexes()
			elif flag == '--dT':
				tableInstance.dropTable()
			elif flag == '--cT':
				tableInstance.createTable()
			elif flag == '--ci':
				tableInstance.createIndexes()
			elif flag == '--ck':
				tableInstance.createKeys()
			elif flag == '--ct':
				tableInstance.createTriggers()
			else:
				# should not happen
				bailout ('Unknown option (%s)' % flag, True)
	return

###--- Classes ---###

# Note that the SQL to create an index should contain two %s instances, one
# for the index name and the other for the table name.  Also note that this
# class will automatically prefix your index name with the name of the table,
# to help ensure unique index names for you.  For example:
#	{ 'mySymbolIndex' : 'create index %s on %s (symbol)' }

class Table:
	def __init__ (self, 
		name,		# string; table name
		createStatement,# string; SQL to create table (leave a %s
				# ...where the table name should be filled in)
		indexes={},	# dict; index name -> SQL to create index
				# ...(see comment above)
		keys={},	# dict; related table name ->list of SQL to
				# ...create keys
		triggers={}	# dict; trigger name -> SQL to create trigger
		):
		# Purpose: constructor
		# Returns: nothing
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing
		# Notes: Neither 'keys' nor 'triggers' are implemented yet.

		self.name = name
		self.createStatement = createStatement
		self.indexes = indexes
		self.keys = keys
		self.triggers = triggers
		return

	def getName (self):
		# Purpose: retrieve the name of the table
		# Returns: string
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		return self.name

	def dropTable (self):
		# Purpose: drop this table from the MySQL database
		# Returns: nothing
		# Assumes: nothing
		# Modifies: see Purpose
		# Throws: propagates any exceptions from the drop operation

		mysql.execute ('DROP TABLE IF EXISTS %s' % self.name)
		return

	def createTable (self):
		# Purpose: creates this table in the MySQL database
		# Returns: nothing
		# Assumes: the table does not exist in the MySQL database
		# Modifies: see Purpose
		# Throws: propagates any exceptions from the create operation

		mysql.execute (self.createStatement)
		return

	def dropIndexes (self):
		# Purpose: drop all indexes for this table
		# Returns: nothing
		# Assumes: the indexes exist
		# Modifies: see Purpose
		# Throws: propagates any exceptions from the drop index
		#	operations

		for index in self.indexes.keys():
			indexName = '%s_%s' % (self.name, index)
			mysql.execute('DROP INDEX %s on %s' % (
				indexName, self.name))
		return

	def createIndexes (self):
		# Purpose: create all indexes for this table
		# Returns: nothing
		# Assumes: the indexes do not exist
		# Modifies: see Purpose
		# Throws: propagates any exceptions from the create index
		#	operations

		for (index, stmt) in self.indexes.items():
			indexName = '%s_%s' % (self.name, index)
			mysql.execute(stmt % (indexName, self.name))
		return

	def dropKeys (self):
		# Purpose: drop all foreign key relationships for this table
		#	(not yet implemented)
		# Returns:
		# Assumes:
		# Modifies:
		# Throws:

		return

	def createKeys (self):
		# Purpose: create all foreign key relationships for this table
		#	(not yet implemented)
		# Returns:
		# Assumes:
		# Modifies:
		# Throws:

		return

	def dropTriggers (self):
		# Purpose: drop all triggers for this table
		#	(not yet implemented)
		# Returns:
		# Assumes:
		# Modifies:
		# Throws:

		return

	def createTriggers (self):
		# Purpose: create all triggers for this table
		#	(not yet implemented)
		# Returns:
		# Assumes:
		# Modifies:
		# Throws:

		return
