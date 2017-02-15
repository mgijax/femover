#!/usr/local/bin/python

import Table

# contains data definition information for the mapping_id table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'mapping_id'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	experiment_key	int		not null,
	logical_db		text	null,
	acc_id			text	null,
	preferred		int		not null,
	private			int		not null,
	sequence_num	int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'acc_id' : 'create index %s on %s (acc_id)',
	}

# column name -> (related table, column in related table)
keys = {
	'experiment_key' : ('mapping_experiment', 'experiment_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('experiment_key', 'create index %s on %s (experiment_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'table of IDs (both primary and secondary) for mapping experiments',
	Table.COLUMN : {
		'unique_key' : 'generated primary key for this table (no other purpose)',
		'experiment_key' : 'foreign key to the mapping_experiment table, identifying the experiment for the ID',
		'logical_db' : 'entity assigning the ID',
		'acc_id' : 'ID for the experiment',
		'preferred' : '1 if this is the preferred ID for this experiment/logical db pair, 0 if not',
		'private' : '1 if this ID is considered private, 0 if public',
		'sequence_num' : 'pre-computed sequence number for ordering IDs of an experiment',
		},
	Table.INDEX : {
		'acc_id' : 'fast lookup by ID',
		'experiment_key' : 'clustered index to group all IDs of an experiment together for fast access',
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
