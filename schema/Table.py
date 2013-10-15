#!/usr/local/bin/python

import sys
import os
import getopt
import config
import dbManager
import Dispatcher
import re
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
	--sk : show 'create foreign key' statements
	--sc : show 'comment' statements
	--sci : show 'create index' statement for clustering index
	--scl : show 'cluster' statement
	--opt : optimize table (vacuum and analyze, for postgres)
    Notes:
    	1. At least one command-line option must be specified.
	2. If one or more operations fail, this script will exit with a
	   non-zero exit code.  The state of the database at that point
	   is not guaranteed to be correct.
	3. In the case of multiple command-line options, they are executed
	   in a certain order (left to right):
		--dk, --di, --dt, --ct, --lf, --ci, --ck, --si, --sk, --sc,
		--sci, --scl, --opt
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

# regex
varchar_field_re = re.compile('varchar\(\d*\)')

# types of comments allowed

TABLE = 'table'
COLUMN = 'column'
INDEX = 'index'

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

def optimizeTable (myTable	# Table object upon which to operate
	):
	if config.TARGET_TYPE == 'mysql':
		pass
	elif config.TARGET_TYPE == 'postgres':
		pgDispatcher = Dispatcher.Dispatcher()
		script = os.path.join (config.CONTROL_DIR,
			'optimizeTablePostgres.sh')
		id = pgDispatcher.schedule (
			'%s %s %s %s %s %s' % (
				script,
				config.TARGET_HOST,
				config.TARGET_DATABASE,
				config.TARGET_USER,
				config.TARGET_PASSWORD,
				myTable.getName() ) )
		pgDispatcher.wait()
		if pgDispatcher.getReturnCode(id):
			bailout ('Failed to optimize %s table in postgres' % \
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
	flagOrder = '--dk --di --dt --ct --lf --ci --ck --si --sk --sc --sci --scl --opt'.split()

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
			elif flag == '--sk':
				stmts = myTable.getForeignKeyCreationStatements()
				for stmt in stmts:
					print stmt
			elif flag == '--sc':
				for stmt in myTable.getCommentStatements():
					print stmt
			elif flag == '--sci':
				index = myTable.getClusteredIndexStatement()
				if index:
					print index
			elif flag == '--scl':
				cluster = myTable.getClusterStatement()
				if cluster:
					print cluster
			elif flag == '--opt':
				optimizeTable (myTable)
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
				#    where the table name should be filled in)
		indexes={},	# dict; index name -> SQL to create index
				#    (see comment above)
		keys={},	# dict; column in this table -> (related
				#    table name, column in related table)
		comments={},	# dict; keys are from [ 'table', 'column',
				#    and 'index' ].  'table' references a
				#    string.  Other two reference a dictionary
				#    where { item name : comment }.
		clusteredIndex = None	# 2-item tuple (index name, SQL to
					# create the index)
		):
		# Purpose: constructor
		# Returns: nothing
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing
		# Notes: 'keys' is not implemented yet.

		# replace all varchar fields with 'text' fields
		if config.TARGET_TYPE == 'postgres':
			createstatement = varchar_field_re.sub('text',createStatement)
		self.name = name
		self.createStatement = createStatement
		self.indexes = indexes
		self.fKeys = keys
		self.comments = comments
		self.clusteredIndex = clusteredIndex
		return

	def _indexName (self, index):
		# get the unique name for the given 'index', internal to the
		# database

		return '%s_%s' % (self.name, index)

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

		DBM.execute ('DROP TABLE IF EXISTS %s CASCADE' % self.name)
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
			DBM.execute('DROP INDEX %s on %s' % (
				self._indexName(index), self.name))
		DBM.commit()
		return

	def getIndexCreationStatements (self):
		stmts = []
		for (index, stmt) in self.indexes.items():
			stmts.append(stmt % (self._indexName(index),
				self.name))
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

		cmd = '''ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s_%s_fk CASCADE'''

		for keyCol in self.fKeys.keys():
			DBM.execute (cmd % (self.name, self.name, keyCol))
		DBM.commit()
		return

	def getForeignKeyCreationStatements (self):
		cmd = '''ALTER TABLE %s ADD CONSTRAINT %s_%s_fk FOREIGN KEY (%s) REFERENCES %s (%s)'''

		stmts = []
		for (keyCol, (fkTable, fkKeyCol)) in self.fKeys.items():
			stmts.append (cmd % (self.name, self.name, keyCol,
				keyCol, fkTable, fkKeyCol))
		return stmts

	def createKeys (self):
		# Purpose: create all foreign key relationships for this table
		#	(not yet implemented)
		# Returns:
		# Assumes:
		# Modifies:
		# Throws:

		for stmt in self.getForeignKeyCreationStatements():
			DBM.execute(stmt)
		DBM.commit()
		return

	def getCommentStatements (self):
		cmd = """COMMENT ON %s %s IS '%s'"""

		stmts = []

		if self.comments.has_key(TABLE):
			stmts.append (cmd % ('TABLE', self.name,
				self.comments[TABLE]) )

		if self.comments.has_key(COLUMN):
			for (col, comment) in self.comments[COLUMN].items():
				colName = '%s.%s' % (self.name, col)
				stmts.append (cmd % ('COLUMN', colName,
					comment))

		if self.comments.has_key(INDEX):
			for (idx, comment) in self.comments[INDEX].items():
				stmts.append (cmd % ('INDEX',
					self._indexName(idx), comment))
		return stmts

	def getClusteredIndexStatement (self):
		if self.clusteredIndex == None:
			return

		(index, cmd) = self.clusteredIndex
		return cmd % (self._indexName(index), self.name)

	def getClusterStatement (self):
		if self.clusteredIndex == None:
			return

		(index, cmd) = self.clusteredIndex
		return 'CLUSTER %s USING %s' % (self.name,
			self._indexName(index) )
