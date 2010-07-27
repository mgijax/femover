#!/usr/local/bin/python

import sys
import os
import getopt
import config
import dbManager
import Dispatcher

###--- Globals ---###

# Usage statement for all Table scripts, when executed from the command-line
USAGE = '''Usage: %s [--dt|--ct|--di|--ci|--dk|--ck|--dt|--ct|--lf <file>]
    Options:
	--dt : drop table (with its indexes and keys)
	--ct : create table
	--di : drop indexes
	--ci : create indexes
	--dk : drop foreign keys
	--ck : create foreign keys
	--lf : load file (with full path to file specified)
	--si : show 'create index' statements
    Notes:
    	1. At least one command-line option must be specified.
	2. If one or more operations fail, this script will exit with a
	   non-zero exit code.  The state of the database at that point
	   is not guaranteed to be correct.
	3. In the case of multiple command-line options, they are executed
	   in a certain order (left to right):
		--dk, --di, --dt, --ct, --lf, --ci, --ck, --si
''' % os.path.basename(sys.argv[0])

# exception raised if things go wrong
Error = 'Table.Error'

if config.TARGET_TYPE == 'mysql':
	#print 'MySQL: (%s, %s, %s, %s)' % (config.TARGET_HOST,
	#	config.TARGET_DATABASE, config.TARGET_USER,
	#	config.TARGET_PASSWORD)
	DBM = dbManager.mysqlManager (config.TARGET_HOST,
		config.TARGET_DATABASE, config.TARGET_USER,
		config.TARGET_PASSWORD)
elif config.TARGET_TYPE == 'postgres':
	#print 'Postgres: (%s, %s, %s, %s)' % (config.TARGET_HOST,
	#	config.TARGET_DATABASE, config.TARGET_USER,
	#	config.TARGET_PASSWORD)
	DBM = dbManager.postgresManager (config.TARGET_HOST,
		config.TARGET_DATABASE, config.TARGET_USER,
		config.TARGET_PASSWORD)
else:
	raise Error, 'Unknown value for config.TARGET_TYPE'

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

def loadFile (filename,	# string; full path to the data file to load
	myTable		# Table object upon which to operate
	):
	if not filename:
		bailout ('No input file for --lf', True)
	if not os.path.exists(filename):
		bailout ('Cannot find file: %s' % filename, True)

	if config.TARGET_TYPE == 'mysql':
		stmt = "load data local infile '%s' into table %s columns terminated by '&=&' lines terminated by '#=#\n'" % (filename, myTable.getName())
		DBM.execute (stmt)
		DBM.commit()

	elif config.TARGET_TYPE == 'postgres':
		pgDispatcher = Dispatcher.Dispatcher()
		script = os.path.join (config.CONTROL_DIR,
			'bulkLoadPostgres.sh')
		id = pgDispatcher.schedule (
			'%s %s %s %s %s %s %s' % (
				script,
				config.TARGET_HOST,
				config.TARGET_DATABASE,
				config.TARGET_USER,
				config.TARGET_PASSWORD,
				filename,
				myTable.getName() ) )
		pgDispatcher.wait()
		if pgDispatcher.getReturnCode(id):
			bailout ('Failed to load %s table in postgres' % \
				myTable.getName() )
	else:
		bailout ('Unknown config.TARGET_TYPE : %s' % \
			config.TARGET_TYPE)
	return

def main (
	myTable		# Table object upon which to operate
	):
	# Purpose: generic main program for all Table subclasses
	# Returns: nothing
	# Assumes: nothing
	# Modifies: table keys, indexes, and definition in the MySQL
	#	database
	# Throws: propagates any exceptions raised

	# at least one command-line argument must be specified

	if len(sys.argv) == 1:
		bailout ('No options were specified', True)

	# command-line flags specified by the user
	userFlags = sys.argv[1:]

	# order in which we must execute the user's specified flags
	flagOrder = '--dk --di --dt --ct --lf --ci --ck --si'.split()

	# check that all user-specified flags are recognized
	priorFlag = ''
	inputFile = None
	for flag in userFlags:
		if not flag in flagOrder:
			if priorFlag == '--lf':
				inputFile = flag
			else:
				bailout ('Unknown option (%s)' % flag, True)
		priorFlag = flag

	# execute the user's flags in the proper order
	for flag in flagOrder:
		if flag in userFlags:
			if flag == '--dk':
				myTable.dropKeys()
			elif flag == '--di':
				myTable.dropIndexes()
			elif flag == '--dt':
				myTable.dropTable()
			elif flag == '--ct':
				myTable.createTable()
			elif flag == '--ci':
				myTable.createIndexes()
			elif flag == '--ck':
				myTable.createKeys()
			elif flag == '--lf':
				loadFile (inputFile, myTable)
			elif flag == '--si':
				stmts = myTable.getIndexCreationStatements()
				for stmt in stmts:
					print stmt
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
		keys={}		# dict; related table name ->list of SQL to
				# ...create keys
		):
		# Purpose: constructor
		# Returns: nothing
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing
		# Notes: 'keys' is not implemented yet.

		self.name = name
		self.createStatement = createStatement
		self.indexes = indexes
		self.keys = keys
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

		DBM.execute ('DROP TABLE IF EXISTS %s' % self.name)
		DBM.commit()
		return

	def createTable (self):
		# Purpose: creates this table in the MySQL database
		# Returns: nothing
		# Assumes: the table does not exist in the MySQL database
		# Modifies: see Purpose
		# Throws: propagates any exceptions from the create operation

		DBM.execute (self.createStatement)
		DBM.commit()
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
			DBM.execute('DROP INDEX %s on %s' % (
				indexName, self.name))
		DBM.commit()
		return

	def getIndexCreationStatements (self):
		stmts = []
		for (index, stmt) in self.indexes.items():
			indexName = '%s_%s' % (self.name, index)
			stmts.append(stmt % (indexName, self.name))
		return stmts

	def createIndexes (self):
		# Purpose: create all indexes for this table
		# Returns: nothing
		# Assumes: the indexes do not exist
		# Modifies: see Purpose
		# Throws: propagates any exceptions from the create index
		#	operations

		for stmt in self.getIndexCreationStatements():
			DBM.execute(stmt)
		DBM.commit()
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
