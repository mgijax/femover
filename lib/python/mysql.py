import MySQLdb
import os
import config

###--- Globals ---###

USER = None		# string; username for MySQL database
HOST = None		# string; hostname for server w/ MySQL database
DATABASE = None		# string; name of database within MySQL db server
PASSWORD = None		# string; user's MySQL password

CONNECTION = None	# MySQLdb.Connection; shared for multiple queries

Error = 'mysql.Error'	# string; exception to be raised in this module

###--- Functions ---###

def readPasswordFile (
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

def getConnection (
	host=None, 		# string; host for MySQL database server
	database=None, 		# string; name of database w/in MySQL server
	user=None, 		# string; username for MySQL
	password=None,		# string; password to go with 'username'
	passwordFile=None	# string; path to password file
	):
	# Purpose: to open a new connection to the MySQL database server
	#	specified in the parameters
	# Returns: MySQLdb.Connection
	# Assumes: nothing
	# Modifies: opens a database connection
	# Throws: Exception if we cannot get a connection
	# Notes: 1. Use either 'password' or 'passwordFile'.  If both are
	#	specified, then only 'password' is used.  2. For any
	#	unspecified parameters, we use as defaults the globals USER,
	#	HOST, DATABASE, and PASSWORD.

	if not host:
		host = HOST
	if not database:
		database = DATABASE
	if not user:
		user = USER
	if not password:
		if not passwordFile:
			password = PASSWORD
		else:
			password = readPasswordFile(passwordFile)

	try:
		connection = MySQLdb.connect (host=host, user=user,
			passwd=password, db=database, local_infile=1)
	except:
		raise Exception (Error,
			'Cannot get connection to %s:%s as %s' % (host,
				database, user) )
	return connection

def initialize (
	host, 			# string; host for MySQL database server
	database, 		# string; name of database w/in MySQL server
	user, 			# string; username for MySQL
	password=None,		# string; password to go with 'username'
	passwordFile=None	# string; path to password file
	):
	# Purpose: initialize the global defaults for the given parameters;
	#	these defaults will be used for any newly instantiated
	#	connections
	# Returns: nothing
	# Assumes: nothing
	# Modifies: alters globals HOST, USER, DATABASE, and PASSWORD
	# Throws: propagates Exception is we cannot ready from 'passwordFile'

	global HOST, USER, DATABASE, PASSWORD

	HOST = host
	DATABASE = database
	USER = user

	if password:
		PASSWORD = password
	elif passwordFile:
		PASSWORD = readPasswordFile (passwordFile)
	return

def execute (
	cmd		# string; SQL statement to be executed
	):
	# Purpose: use the shared, global connection to execute the given
	#	MySQL 'cmd'
	# Returns: tuple of (dictionary, list)
	#	1. dictionary maps from column name to column number, and
	#		vice versa
	#	2. list of other lists, one per row.  Each row (each contained
	#		list) has one value for each column returned by the
	#		query.
	# Assumes: nothing
	# Modifies: nothing
	# Throws: propagates Exception for any errors encountered

	global CONNECTION

	# instantiate a connection, if we have not yet done so

	if not CONNECTION:
		CONNECTION = getConnection()

	# get a cursor for executing the desired SQL statement

	cursor = CONNECTION.cursor()

	try:
		cursor.execute (cmd)
	except MySQLdb.Error, e:
		CONNECTION.rollback()
		cursor.close()
		raise Exception (Error,
			'Command failed (%s) Error %d : %s' % (cmd,
			e.args[0], e.args[1]) )

	# convert column names in cursor.description list into a dictionary
	# mapping from name to index and from index to name

	i = 0
	columns = {}
	if cursor.description:
		for tpl in cursor.description:
			columns[tpl[0]] = i
			columns[i] = tpl[0]
			i = i + 1

	# retrieve the data rows and close the cursor

	rows = cursor.fetchall()
	cursor.close()

	return columns, rows

def commit ():
	# Purpose: issue a 'commit' command on the global connection, if one
	#	is open
	# Returns: nothing
	# Assumes: nothing
	# Modifies: global CONNECTION
	# Throws: nothing

	global CONNECTION

	if CONNECTION:
		CONNECTION.commit()
		CONNECTION.close()
	CONNECTION = None
	return

def asSybase (
	columnsAndRows		# tuple, as returned by execute()
	):
	# Purpose: convert the MySQL-format query returns into a Sybase-style
	#	list of dictionaries, as would be returned by db.sql() for a
	#	single SQL statement
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
		for col in row:
			sybRow[columns[i]] = col
			i = i + 1

		sybRows.append (sybRow)

	return sybRows

###--- Module Initialization ---###

# set up this module's default connection data from the product's config file.

initialize (config.MYSQL_HOST, config.MYSQL_DATABASE, config.MYSQL_USER,
	config.MYSQL_PASSWORD)
