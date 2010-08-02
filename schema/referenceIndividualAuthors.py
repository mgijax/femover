#!/usr/local/bin/python

import Table

# contains data definition information for the referenceIndividualAuthors
# table

###--- Globals ---###

# name of this database table
tableName = 'referenceIndividualAuthors'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	uniqueKey	int NOT NULL,
	referenceKey 	int NOT NULL,
	author    	varchar(255) NULL,
	sequenceNum	int NOT NULL,
	isLast		int NOT NULL,
	PRIMARY KEY(uniqueKey))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'referenceKey' : 'create index %s on %s (referenceKey)',
	'sequenceNum' : 'create index %s on %s (sequenceNum)',
	'isLast' : 'create index %s on %s (isLast)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
