#!/usr/local/bin/python

import Table

# contains data definition information for the markerSynonym table

###--- Globals ---###

# name of this database table
tableName = 'markerSynonym'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	uniqueKey	int	NOT NULL,
	markerKey	int	NOT NULL,
	synonym		varchar(255)	NULL,
	synonymType	varchar(255)	NULL,
	jnumID		varchar(30)	NULL,
	PRIMARY KEY(uniqueKey))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'markerKey' : 'create index %s on %s (markerKey)',
	'synonym' : 'create index %s on %s (synonym)',
	'jnumID' : 'create index %s on %s (jnumID)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
