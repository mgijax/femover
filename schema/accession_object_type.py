#!/usr/local/bin/python

import Table

# contains data definition information for the accession_object_type table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'accession_object_type'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	object_type_key		int		not null,
	object_type		varchar(255)	null,
	PRIMARY KEY(object_type))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'object_type' : 'create index %s on %s (object_type)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
