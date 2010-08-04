#!/usr/local/bin/python

import Table

# contains data definition information for the markerToSequence table

###--- Globals ---###

# name of this database table
tableName = 'markerToSequence'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	uniqueKey	int		NOT NULL,
	markerKey	int		NOT NULL,
	sequenceKey	int 		NOT NULL,
	referenceKey	int		NOT NULL,
	qualifier	varchar(80)	NULL,
	PRIMARY KEY(uniqueKey))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'markerKey' : 'create index %s on %s (markerKey, sequenceKey)',
	'sequenceKey' : 'create index %s on %s (sequenceKey, markerKey)',
	'referenceKey' : 'create index %s on %s (referenceKey)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
