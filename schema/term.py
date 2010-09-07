#!/usr/local/bin/python

import Table

# contains data definition information for the term table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'term'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	term_key	int		not null,
	term		varchar(255)	null,
	primary_id	varchar(30)	null,
	vocab_name	varchar(255)	not null,
	definition	varchar(2048)	null,
	sequence_num	int		null,
	is_root		int		not null,
	is_leaf		int		not null,
	PRIMARY KEY(term_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'primary_id' : 'create index %s on %s (primary_id)',
	'vocab_name' : 'create index %s on %s (vocab_name, is_root)',
	'term' : 'create index %s on %s (term)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
