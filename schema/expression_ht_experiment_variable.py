#!/usr/local/bin/python

import Table

# contains data definition information for the expression_ht_experiment_variable table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_ht_experiment_variable'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	experiment_key	int		not null,
	variable		text	null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'variable' : 'create index %s on %s (variable)',
	}

# column name -> (related table, column in related table)
keys = {
	'experiment_key' : ('expression_ht_experiment', 'experiment_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('experiment_key', 'create index %s on %s (experiment_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'contains the experimental variables for each experiment, 0-n rows per experiment',
	Table.COLUMN : {
		'unique_key' : 'uniquely identifies this record, no other significance',
		'experiment_key' : 'identifies the experiment',
		'variable' : 'experimental variable',
		},
	Table.INDEX : {
		'experiment_key' : 'clustered index, bringing together records for each experiment for fast retrieval',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments,
		clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
