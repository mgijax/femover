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

comments = {
	Table.TABLE : 'petal table for the accession flower; contains displayable object types.  These differ from the standard object types in that they are more specific.  For example, mapping experiments are one object type, but have several displayable object types, one for each type of mapping experiment.',
	Table.COLUMN : {
	    'display_type_key' : 'unique key identifying an object type for display purposes',
	    'display_type' : 'the displayable object type',
		},
	Table.INDEX : {
	    'display_type' : 'for searching by the displayable object type',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, comments)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
