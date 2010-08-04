#!/usr/local/bin/python

import Table

# contains data definition information for the alleleCounts table

###--- Globals ---###

# name of this database table
tableName = 'alleleCounts'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	alleleKey	int	NOT NULL,
	markerCount	int	NULL,
	PRIMARY KEY(alleleKey))''' % tableName

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
