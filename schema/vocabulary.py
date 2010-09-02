#!/usr/local/bin/python

import Table

# contains data definition information for the vocabulary table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'vocabulary'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	vocab_key	int		not null,
	vocab_name	varchar(255)	not null,
	term_count	int		not null,
	is_simple	int		not null,
	max_depth	int		not null,
	PRIMARY KEY(vocab_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'vocab_name' : 'create index %s on %s (vocab_name)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
