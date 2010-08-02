#!/usr/local/bin/python

import Table

# contains data definition information for the sequenceSource table

###--- Globals ---###

# name of this database table
tableName = 'sequenceSource'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	uniqueKey	int		NOT NULL,
	sequenceKey	int		NOT NULL,
	strain		varchar(255)	NULL,
	tissue		varchar(80)	NULL,
	age		varchar(50)	NULL,
	sex		varchar(100)	NULL,
	cellLine	varchar(100)	NULL,
	PRIMARY KEY(uniqueKey))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'sequenceKey' : 'create index %s on %s (sequenceKey)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
