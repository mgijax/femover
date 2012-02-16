#!/usr/local/bin/python

import Table

# contains data definition information for the actual_database table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'actual_database'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	actualdb_key	int		not null,
	logical_db	varchar(80)	null,
	actual_db	varchar(80)	null,
	url		varchar(255)	null,
	sequence_num	int		not null,
	PRIMARY KEY(actualdb_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'logical_db' : 'create index %s on %s (logical_db)',
	}

# column name -> (related table, column in related table)
keys = {}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
