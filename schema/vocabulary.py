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
	vocab_name	text	not null,
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

comments = {
	Table.TABLE : 'defines the standard set of vocabularies and some basic data about the structure of each',
	Table.COLUMN : {
		'vocab_key' : 'unique key for this vocabulary',
		'vocab_name' : 'name of this vocabulary',
		'term_count' : 'number of terms in this vocabulary',
		'is_simple' : '1 if this is a flat (simple) vocabulary, or 0 if it is a DAG',
		'max_depth' : 'maximum depth of a leaf node in this vocabulary',
		},
	Table.INDEX : {
		'vocab_name' : 'quick lookup by vocabulary name',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, {}, comments)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
