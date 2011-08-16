#!/usr/local/bin/python

import Table

# contains data definition information for the reference_sequence_num table

###--- Globals ---###

# name of this database table
tableName = 'reference_sequence_num'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	reference_key	int		not null,
	by_date		int		not null,
	by_author	int		not null,
	by_primary_id	int		not null,
	by_title	int		not null,
	PRIMARY KEY(reference_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'reference_key' : ('reference', 'reference_key') }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
