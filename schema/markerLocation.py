#!/usr/local/bin/python

import Table

# contains data definition information for the markerLocation table

###--- Globals ---###

# name of this database table
tableName = 'markerLocation'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	uniqueKey		int		NOT NULL,
	markerKey		int		NOT NULL,
	sequenceNum		int		NOT NULL,
	chromosome		varchar(8)	NULL,
	cmOffset		float		NULL,
	cytogeneticOffset	varchar(20)	NULL,
	startCoordinate		float		NULL,
	endCoordinate		float		NULL,
	buildIdentifier		varchar(30)	NULL,
	locationType		varchar(20)	NOT NULL,
	mapUnits		varchar(50)	NULL,
	provider		varchar(255)	NULL,
	PRIMARY KEY(uniqueKey))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'markerKey' : 'create index %s on %s (markerKey, sequenceNum)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
