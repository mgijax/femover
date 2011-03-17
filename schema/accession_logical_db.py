#!/usr/local/bin/python

import Table

# contains data definition information for the accession_logical_db table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'accession_logical_db'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	logical_db_key		int		not null,
	logical_db		varchar(255)	not null,
	PRIMARY KEY(logical_db_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'logical_db' : 'create index %s on %s (logical_db)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
