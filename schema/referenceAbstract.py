#!/usr/local/bin/python

import Table

# contains data definition information for the referenceAbstract table

###--- Globals ---###

# name of this database table
tableName = 'referenceAbstract'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	referenceKey	int NOT NULL,
	abstract	text NULL,
	PRIMARY KEY(referenceKey))''' % tableName

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
