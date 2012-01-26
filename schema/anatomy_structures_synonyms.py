#!/usr/local/bin/python

import Table

# contains data definition information for the anatomy_structures_synonyms
# table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'anatomy_structures_synonyms'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	structure		varchar(80)	null,
	synonym			varchar(80)	null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'structure' : 'create index %s on %s (structure)',
	'synonym' : 'create index %s on %s (synonym)',
	}

keys = {}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
