#!/usr/local/bin/python

import Table
import logger

# contains data definition information for the database info table

###--- Classes ---###

class databaseInfoTable (Table.Table):
	def setInfo (self, name, value):
#		logger.debug ('Called setInfo(%s, %s)' % (name, value))

		dbm = Table.DBM
		cols, rows = dbm.execute ('''select uniqueKey
			from databaseInfo
			where name = '%s' ''' % name)

		if len(rows) > 0:
			nextKey = rows[0][0]
			cmd = '''update databaseInfo set value = '%s'
				where uniqueKey = %d''' % (value, nextKey)
		else:
			cols, rows = dbm.execute ('''select max(uniqueKey)
				from databaseInfo''')
			if (len(rows) > 0) and (rows[0][0] != None):
				nextKey = rows[0][0] + 1
			else:
				nextKey = 1

			cmd = '''insert into databaseInfo (uniqueKey, name,
					value) values (%d, '%s', '%s')''' % (
						nextKey, name, value)
		dbm.execute(cmd)
		dbm.commit()
#		logger.debug ('Added %s = %s as key %d' % (name, value,
#			nextKey))
		return

###--- Globals ---###

# name of this database table
tableName = 'databaseInfo'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	uniqueKey	int		not null,
	name		varchar(40)	not null,
	value		varchar(255)	null,
	PRIMARY KEY(uniqueKey))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# global instance of this Table object
table = databaseInfoTable (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
