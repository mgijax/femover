#!/usr/local/bin/python
# 
# provides an execute() method for executing queries against Sybase, MySQL, or
#	Postgres as a source database.  Ensures that the caller does not need
#	to know which is which.

import config
import logger
import types
import dbManager

###--- Globals ---###

Error = 'dbAgnostic.error'	# exception raised by this module
SOURCE_DB = config.SOURCE_TYPE	# either sybase, mysql, or postgres
DBM = None			# dbManager object for postgres/mysql access
LOADED_SYBASE = False

# set up our database connectivity

if SOURCE_DB == 'sybase':
	# Where possible, use the FreeTDS library for Sybase access.  If not
	# possible, use our MGI db.py interface to Sybase's client libraries.
	try:
		import freedb
		db = freedb
		logger.debug ('Loaded FreeTDS freedb library')
	except:
		import db
		db.set_sqlLogin (config.SOURCE_USER, config.SOURCE_PASSWORD,
			config.SOURCE_HOST, config.SOURCE_DATABASE)
		logger.debug ('Loaded MGI-written db library')

	LOADED_SYBASE = True
	db.useOneConnection(1)

elif SOURCE_DB == 'postgres':
	DBM = dbManager.postgresManager (config.SOURCE_HOST,
		config.SOURCE_DATABASE, config.SOURCE_USER,
		config.SOURCE_PASSWORD)
	logger.debug ('Created postgresManager')

elif SOURCE_DB == 'mysql':
	DBM = dbManager.mysqlManager (config.SOURCE_HOST,
		config.SOURCE_DATABASE, config.SOURCE_USER,
		config.SOURCE_PASSWORD)
	logger.debug ('Created mysqlManager')
else:
	raise Error, 'Unknown value for config.SOURCE_TYPE : %s' % SOURCE_DB

###--- Functions ---###

def execute (cmd):
	# Purpose: execute the given SQL 'cmd' against the source database
	# Returns: two-item tuple of (columns, rows)
	#	columns - list of column names
	#	rows - list of rows, where each row is a list of values in the
	#		order corresponding to the column names in 'columns'

	# if using either postgres or mysql, our dbManager object will handle
	# all necessary database interaction

	if DBM:
		return DBM.execute(cmd)

	# if not using a dbManager (ie- we are querying Sybase), then convert
	# the Sybase-style return to the dbManager-style return

	if not LOADED_SYBASE:
		raise Error, 'Failed to load Sybase driver'

	results = db.sql (cmd, 'auto')
	columns = []
	rows = []

	if results:
		columns = results[0].keys()
		columns.sort()

		for sybRow in results:
			row = []
			for col in columns:
				row.append (sybRow[col])
			rows.append (row)

	return columns, rows

def columnNumber (columns, columnName):
	# find the position of 'columnName' in the list of 'columns'
	if columns and type(columns[0])==type([]):
		columns = columns[0]

	if columnName in columns:
		return columns.index(columnName)

	# Postgres returns lowercase fieldnames, so check for that

	c = columnName.lower()
	if c not in columns:
		logger.info('1234columns = %s'%columns);
		logger.info('column = %s'%c);
		logger.error ('Column %s (%s) is not in %s' % (columnName, c, 
			', '.join (columns) ) )
		raise Error, 'Unknown column name: %s' % columnName

	return columns.index(c)

def tuplesToLists (rows):
	# 'rows' is a list of query results.  If these results are tuples,
	# then convert them to lists instead.

	if len(rows) == 0:
		return rows

	if type(rows[0]) == types.ListType:
		return rows

	r = []
	for row in rows:
		r.append(list(row))
	return r

def mergeResultSets (cols1, rows1, cols2, rows2):
	# return a unified (cols, rows) pair, accounting for the fact
	# that column ordering in cols1 and cols2 may be different.
	# Assumes that the column names in cols1 and cols2 are
	# the same names, even if they appear in a different order.
	# Note: This function can alter rows1.

	colsMatch = True

	for c in cols1:
		if (c not in cols2):
			raise Error, 'Item "%s" not in cols2: %s' % \
				(c, str(cols2))

		if (cols1.index(c) != cols2.index(c)):
			colsMatch = False
			break

	# easy case: the columns are already in the same order, so we
	# can just concatenate the lists

	if colsMatch:
		return (cols1, rows1 + rows2)

	# Otherwise, we'll need to ensure we re-order the values in
	# rows2 to match and append those rows to rows1.

	colOrder = []
	for c in cols1:
		if (c not in cols2):
			raise Error, 'Item "%s" not in cols2: %s' % \
				(c, str(cols2))

		colOrder.append(cols2.index(c))

	for row in rows2:
		r = []
		for c in colOrder:
			r.append(row[c])
		rows1.append(r)

	return (cols1, rows1)
