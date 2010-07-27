#!/usr/local/bin/python

import Table

# contains data definition information for the sequenceGeneTrap table

###--- Globals ---###

# name of this database table
tableName = 'sequenceGeneTrap'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	sequenceKey		int		NOT NULL,
	tagMethod		varchar(40)	NOT NULL,
	vectorEnd		varchar(40)	NOT NULL,
	reverseComplement	varchar(40)	NOT NULL,
	goodHitCount		int		NULL,
	pointCoordinate		float		NULL,
	PRIMARY KEY(sequenceKey))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
