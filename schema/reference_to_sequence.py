#!/usr/local/bin/python

import Table

# contains data definition information for the reference_to_sequence table

###--- Globals ---###

# name of this database table
tableName = 'reference_to_sequence'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int		NOT NULL,
	sequence_key	int 	NOT NULL,
	reference_key	int		NOT NULL,
	qualifier	varchar(80)	NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'sequence_key' : 'create index %s on %s (sequence_key)',
	'reference_key' : 'create index %s on %s (reference_key)',
	}

keys = {
	'reference_key' : ('reference', 'reference_key'),
	'sequence_key' : ('sequence', 'sequence_key')
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
