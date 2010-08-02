#!/usr/local/bin/python

import Table

# contains data definition information for the Sequence table

###--- Globals ---###

# name of this database table
tableName = 'sequence'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	sequenceKey	int		NOT NULL,
	sequenceType	varchar(30)	NOT NULL,
	quality		varchar(30)	NULL,
	status		varchar(30)	NOT NULL,
	provider	varchar(255)	NOT NULL,
	organism	varchar(50)	NOT NULL,
	length		int		NULL,
	description	varchar(255)	NULL,
	version		varchar(15)	NULL,
	division	varchar(3)	NULL,
	isVirtual	int		NOT NULL,
	sequenceDate	varchar(12)	NULL,
	recordDate	varchar(12)	NULL,
	primaryID	varchar(30)	NULL,
	logicalDB	varchar(80)	NULL,
	library		varchar(255)	NULL,
	PRIMARY KEY(sequenceKey))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'primaryID' : 'create index %s on %s (primaryID)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
