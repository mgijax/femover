# Name: dbManager.py
# Purpose: to provide a simple, consistent, convenient mechanism for working
#	with MySQL and Postgres database connections

import os
import traceback
import sys

###--- Globals ---###

LOADED_MYSQL_DRIVER = False	# have we loaded the MySQL python module?
LOADED_POSTGRES_DRIVER = False	# have we loaded the Postgres python module?

MYSQL = 'MySQL'			# constant; identifies type of dbManager
POSTGRES = 'Postgres'		# constant; identifies type of dbManager

Error = 'dbManager.Error'    # string; exception to be raised in this module

try:
	import MySQLdb
	LOADED_MYSQL_DRIVER = True
except:
	pass

try:
	import psycopg2
	LOADED_POSTGRES_DRIVER = True
except:
	pass

###--- Classes ---###

class dbManager:
    # Is: a manager for a database connection
    # Has: connection parameters and, optionally, a shared connection to the
    #	database
    # Does: can use the 'dbManager' to either give you back a connection to
    #	manage yourself, or can use its execute() method to use the dbManager's
    #	shared connection (and let it manage the connection for you).  Any
    #	changes made using execute() should either be confirmed (using
    #	commit()) or rolled back (using rollback()).

    def __init__ (self,
	host, 			# string; host for database server
	database, 		# string; name of database w/in the server
	user, 			# string; username for database
	password=None,		# string; password to go with 'username'
	passwordFile=None	# string; path to password file
	):
	# Purpose: constructor
	# Returns: nothing
	# Assumes: nothing
	# Modifies: reads the 'passwordFile' from the file system, if that
	#	parameter is specified
	# Throws: propagates any exceptions raised if we cannot read the
	#	specified password file
	# Notes: must specify either a 'password' or a 'passwordFile'

	self.dbType = None	# must be filled in by subclass's setDbType()
	self._setDbType()	# ...method

	self.sharedConnection = None	# shared Connection object

	# should we return results as a list of dictionaries like Sybase?
	# (default is NO)
	self.returnAsSybase = False

	# connection parameters

	self.host = host
	self.database = database
	self.user = user
	if password:
		self.password = password
	elif passwordFile:
		self.password = __readPasswordFile (passwordFile)
	else:
		raise Error, 'Could not initialize; no password specified'
	return

    def getConnection (self):
	# Purpose: get a new connection to the database
	# Returns: Connection object
	# Assumes: nothing
	# Modifies: opens a database connection
	# Throws: Exception if we cannot make a database connection

	self.__checkDbType()
	try:	
		connection = self._getConnection()
	except:
		(excType, excValue, excTraceback) = sys.exc_info()
		traceback.print_exception (excType, excValue, excTraceback)
		raise Exception (Error,
			'Cannot get connection to %s:%s as %s' % (self.host,
				self.database, self.user) )
	return connection

    def execute (self,
	cmd		# string; SQL statement to execute on this dbManager's
			# ...shared connection
	):
	# Purpose: execute the given 'cmd' on the shared connection
	# Returns: natively, a 2-item tuple:
	#	(list of column names, list of lists -- each inner list of a
	#	list of values for the columns for that row)
	# 	or, if operating in 'return as Sybase' mode, returns:
	#	list of dictionaries, each of which has fieldnames as keys
	#	with each mapped to its value for that row
	# Assumes: we have the necessary permissions to execute the database
	#	statement
	# Modifies: could alter database structure or contents, depending on
	#	what 'cmd' is
	# Throws: Exception if we cannot get a connection to use or if we
	#	cannot execute the given 'cmd'

	# instantiate a connection, if we have not yet done so

	if not self.sharedConnection:
		self.sharedConnection = self.getConnection()

	# get a cursor for executing the desired SQL statement

	cursor = self.sharedConnection.cursor()

	try:
		cursor.execute (cmd)
	except StandardError, e:
		self.sharedConnection.rollback()
		cursor.close()
		raise Exception (Error,
			'Command failed (%s) Error: %s' % (cmd, str(e.args)))
			#'Command failed (%s) Error %s : %s' % (cmd,
			#e.args[0], e.args[1]) )

	# convert column names in cursor.description list into a simple list
	# of field names

	columns = []
	if cursor.description:
		for tpl in cursor.description:
			columns.append (tpl[0])

	# if we did not find any columns, then there are no rows to retrieve

	if not columns:
		cursor.close()
		return None, None

	# retrieve the data rows and close the cursor

	rows = cursor.fetchall()
	cursor.close()

	# if we need to convert our return value to mimic our db.sql()
	# function for Sybase, then convert the columns and rows...

	if self.returnAsSybase:
		return __asSybase ( (columns, rows) )
	return columns, rows

    def getReturnAsSybase (self):
	# Purpose: examine the flag for whether our results are Sybase-style
	#	(True) or not (False)
	# Returns: boolean
	# Assumes: nothing
	# Modifies: nothing
	# Throws: nothing

	return self.returnAsSybase

    def setReturnAsSybase (self,
	flag				# boolean; True for Sybase-style,
					# ...False if not
	):
	# Purpose: configure this dbManager to either return Sybase-style
	#	results (True) or not (False)
	# Returns: nothing
	# Assumes: nothing
	# Modifies: nothing
	# Throws: nothing

	self.returnAsSybase = flag
	return

    def commit (self):
	# Purpose: issue a 'commit' command on the shared connection, if one
	#	is open
	# Returns: nothing
	# Assumes: nothing
	# Modifies: commits any outstanding changes to the database for the
	#	current shared connection
	# Throws: nothing

	if self.sharedConnection:
		self.sharedConnection.commit()
		self.sharedConnection.close()
	self.sharedConnection = None
	return

    def rollback (self):
	# Purpose: issue a 'rollback' command on the shared connection, if one
	#	is open
	# Returns: nothing
	# Assumes: nothing
	# Modifies: rolls back any outstanding changes to the database for the
	#	current shared connection
	# Throws: nothing

	if self.sharedConnection:
		self.sharedConnection.rollback()
		self.sharedConnection.close()
	self.sharedConnection = None
	return

    def __checkDbType (self):
	# Purpose: check that our dbManager knows what type of database it
	#	should comminicate with; this is used internally by the
	#	dbManager class to ensure that 'self' is of a subclass of
	#	dbManager, rather than the parent class itself
	# Returns: nothing
	# Assumes: nothing
	# Modifies: nothing
	# Throws: Error if it does not know the type of database

	if not self.dbType:
		raise Error, 'Cannot instantiate dbManager class ' + \
			'directly; must use a subclass'
	return

    def _getConnection (self):
	# Purpose: to get a database connection; this is an internal method
	#	that must be implemented in subclasses of dbManager
	# Returns: a database connection
	# Assumes: nothing
	# Modifies: nothing
	# Throws: Error if this method was not re-implemented in 'self'

	raise Error, 'Must implement _getConnection() in a subclass'

    def _setDbType (self):
	# Purpose: to set the database type for this dbManager; this is an
	#	internal method	that must be implemented in subclasses of
	#	dbManager
	# Returns: nothing
	# Assumes: nothing
	# Modifies: nothing
	# Throws: Error if this method was not re-implemented in 'self'

	raise Error, 'Must implement _setDbType() in a subclass'

class mysqlManager (dbManager):
    # Is: a dbManager that knows how to interact with MySQL
    # Has: see dbManager
    # Does: see dbManager

    def _setDbType (self):
	# Purpose: to set this dbManager's database type to be MYSQL
	# Other: see dbManager._setDbType() for other comments

	self.dbType = MYSQL
	return

    def _getConnection (self):
	# Purpose: to get a connection to a MySQL database
	# Returns: connection object
	# Assumes: nothing
	# Modifies: nothing
	# Throws: propagates any exceptions from MySQLdb.connect() method

	if not LOADED_MYSQL_DRIVER:
		raise Error, \
		    'Cannot get connection; MySQLdb driver was not loaded'

	return MySQLdb.connect (host=self.host, user=self.user,
		passwd=self.password, db=self.database, local_infile=1)

class postgresManager (dbManager):
    # Is: a dbManager that knows how to interact with Postgres
    # Has: see dbManager
    # Does: see dbManager

    def _setDbType (self):
	# Purpose: to set this dbManager's database type to be POSTGRES
	# Other: see dbManager._setDbType() for other comments

	self.dbType = POSTGRES
	return

    def _getConnection (self):
	# Purpose: to get a connection to a Postgres database
	# Returns: connection object
	# Assumes: nothing
	# Modifies: nothing
	# Throws: propagates any exceptions from psycopg2.connect() method

	if not LOADED_POSTGRES_DRIVER:
		raise Error, \
		    'Cannot get connection; psycopg2 driver was not loaded'

	return psycopg2.connect (host=self.host, user=self.user,
		password=self.password, database=self.database)

###--- Functions ---###

def __readPasswordFile (
	file		# string; path to file containing a password
	):
	# Purpose: retrieve the password contained in the file identified by
	#	its given path
	# Returns: string; the password from the file
	# Assumes: nothing
	# Modifies: nothing
	# Throws: Exception if we cannot read the password file

	if not os.path.exists(file):
		raise Exception(Error, 'Unknown password file: %s' % file)
	try:
		fp = open(file, 'r')
		password = fp.readline().trim()
		fp.close()
	except:
		raise Exception(Error, 'Cannot read password file: %s' % file)
	return password

def __asSybase (
	columnsAndRows		# tuple, as returned by execute()
	):
	# Purpose: convert the dbManager-format query returns into a
	#	Sybase-style list of dictionaries, as would be returned by
	#	db.sql() for a single SQL statement
	# Returns: list of dictionaries.  Each dictionary is for one row of
	#	data, with fieldnames as keys and field values as values.
	# Assumes: nothing
	# Modifies: nothing
	# Throws: nothing

	columns, rows = columnsAndRows

	sybRows = []

	for row in rows:
		sybRow = {}

		i = 0
		for col in columns:
			sybRow[col] = row[i]
			i = i + 1

		sybRows.append (sybRow)

	return sybRows
