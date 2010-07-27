#!/usr/local/bin/python

# Purpose: provides (very) limited emulation of db.py module using FreeTDS
#	package, rather than Sybase DB and CT libraries.  This targets the
#	most common use by the front-end -- calls to sql() using the 'auto'
#	parameter.
# Usage: as a Python library
# Notes: By default, we use a new database connection for each call to sql(),
#	even if that call contains multiple SQL commands.  You can call
#	useOneConnection(True) to ensure that a single connection is used
#	for all sql() calls.

import os
import Sybase
import sys
import config
import types

###--- Configuration ---###

if not os.environ.has_key ('SYBASE'):
	os.environ['SYBASE'] = '/opt/sybase/15'

###--- Globals ---###

# standard exception raised in this module
error = 'dbTable.error'	

# shared connection when operating in one-connection mode
connection = None	

# boolean; share one connection across multiple queries (True) or not (False)
oneConnection = False

###--- Functions ---###

def useOneConnection (
	flag		# boolean; True to share a connection, False to not
	):
	# Purpose: to inform the module whether to use a new connection for
	#	each query (False) or to share one connection (True)
	# Returns: nothing
	# Assumes: nothing
	# Modifies: global 'oneConnection'
	# Throws: nothing

	global oneConnection

	oneConnection = flag
	return

def sql (
	cmd,			# string / list of strings; each a SQL command
	autoFlag = 'auto'	# optional string; to catch standard auto flag
	):
	# Purpose: to issue the query (or queries) contained in 'cmd'
	# Returns: either a list of dictionaries (if cmd is a string) or a
	#	list of lists of dictionaries (if cmd is a list)
	# Assumes: we can get a connection to Sybase
	# Modifies: global 'connection'
	# Throws: propagates all exceptions

	global connection

	# if we don't already have a globally shared connection, get one

	if not connection:
		connection = Sybase.connect (config.SOURCE_HOST,
			config.SOURCE_USER, config.SOURCE_PASSWORD,
			config.SOURCE_DATABASE, auto_commit = 1)

	# do we have multiple SQL statements in 'cmd' as a list?

	if type(cmd) == types.ListType:
		multi = True
		cmds = cmd
	else:
		multi = False
		cmds = [ cmd ]

	# list of lists of dictionaries; each sublist contains the returned
	# rows for one query
	results = []

	for cmd in cmds:

		# get a new cursor for this SQL command and execute it

		cursor = connection.cursor()
		cursor.execute (cmd)

		# get the column labels and the row tuples

		columns = cursor.description[:]
		rows = cursor.fetchall()

		# close the cursor, as we finished our database interaction

		cursor.close()

		# list of dictionaries, the results for this query
		dbRows = []

		for row in rows:
			# new dictionary for this particular row
			dict = {}

			# add the name/value pair for each column in the row
			i = 0
			for col in row:
				dict[columns[i][0]] = col
				i = i + 1

			# add this dictionary to our result set
			dbRows.append (dict)

		# add this result set to our list of result sets
		results.append (dbRows)

	# if we're not sharing a connection, close the current one

	if not oneConnection:
		connection.close()
		connection = None

	# and, return the appropriate type of results, depending on whether
	# we ran multiple queries or not

	if multi:
		return results

	return results[0]
