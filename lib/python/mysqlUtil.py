import config
import mysql

def findMaxKey (table, keyName = 'uniqueKey'):
	# Purpose: finds the maximum value for 'keyName' in the given 'table'
	# Returns: integer
	# Assumes: 1. the given 'table' exists; 2. the given 'table' has a
	#	column named according to 'keyName'; 3. 'table' is in the
	#	front-end MySQL database (not Sybase)
	# Modifies: nothing
	# Throws: propagates all exceptions from mysql.execute()

	columns, rows = mysql.execute ('select max(%s) from %s' % (
		keyName, table) )
	return rows[0][0]
