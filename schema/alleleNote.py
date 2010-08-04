#!/usr/local/bin/python

import Table

# contains data definition information for the alleleNote table

###--- Globals ---###

# name of this database table
tableName = 'alleleNote'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	uniqueKey	int	NOT NULL,
	alleleKey	int	NOT NULL,
	noteType	varchar(255)	NOT NULL,
	note		varchar(64000)	NULL,
	PRIMARY KEY(uniqueKey))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'alleleKey' : 'create index %s on %s (alleleKey, noteType)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
