#!/usr/local/bin/python

import Table

# contains data definition information for the reference_individual_authors
# table

###--- Globals ---###

# name of this database table
tableName = 'reference_individual_authors'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int NOT NULL,
	reference_key 	int NOT NULL,
	author    	varchar(255) NULL,
	sequence_num	int NOT NULL,
	is_last		int NOT NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'reference_key' : 'create index %s on %s (reference_key)',
	'sequence_num' : 'create index %s on %s (sequence_num)',
	'is_last' : 'create index %s on %s (is_last)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)