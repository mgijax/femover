#!/usr/local/bin/python

import Table

# contains data definition information for the term_sequence_num table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'term_sequence_num'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	term_key		int	not null,
	by_default		int	not null,
	by_dfs			int	not null,
	by_vocab_dag_term	int	not null,
	PRIMARY KEY(term_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'term_key' : 'create index %s on %s (term_key)',
	}

# column name -> (related table, column in related table)
keys = {}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
