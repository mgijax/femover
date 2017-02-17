#!/usr/local/bin/python

import Table

# contains data definition information for the mapping_table table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'mapping_table'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	mapping_table_key	int		not null,
	experiment_key		int		not null,
	table_type			text	not null,
	sequence_num		int		not null,
	PRIMARY KEY(mapping_table_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {
	'experiment_key' : ('mapping_experiment', 'experiment_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('experiment_key', 'create index %s on %s (experiment_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'Each row defines one table of detailed data for an experiment.',
	Table.COLUMN : {
		'mapping_table_key' : 'generated primary key for this table; no other significance',
		'experiment_key' : 'foreign key to mapping_experiment table, identifying the experiment',
		'table_type' : 'type of table',
		'sequence_num' : 'sequence number for ordering multiple tables for an experiment',
		},
	Table.INDEX : {
		'experiment_key' : 'clustered index to keep tables for the same experiment together on disk for quick retrieval',
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
