#!/usr/local/bin/python

import Table

# contains data definition information for the accession_display_type table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'accession_display_type'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	display_type_key	smallint	not null,
	display_type		varchar(255)	null,
	PRIMARY KEY(display_type_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'display_type' : 'create index %s on %s (display_type)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
