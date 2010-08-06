#!/usr/local/bin/python

import Table

###--- Globals ---###

# name of this database table
tableName = 'probe'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	probeKey    	int	NOT NULL,
	name          	varchar(40) NULL,
	segmentType	varchar(255) NULL,
	primaryID      	varchar(30) NULL,
	logicalDB	varchar(80) NULL,
	cloneID		varchar(30) NULL,
	PRIMARY KEY(probeKey))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'cloneID' : 'create index %s on %s (cloneID)',
	'ID' : 'create index %s on %s (primaryID)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
