#!/usr/local/bin/python

import Table

# contains data definition information for the mapping_hybrid table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'mapping_hybrid'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	hybrid_key			int		not null,
	experiment_key		int		not null,
	band				text	null,
	concordance_type	text	not null,
	PRIMARY KEY(hybrid_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'experiment_key' : 'create index %s on %s (experiment_key)',
	}

# column name -> (related table, column in related table)
keys = {
	'experiment_key' : ('mapping_experiment', 'experiment_key'),
	}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'contains supplemental data for HYBRID mapping experiments',
	Table.COLUMN : {
		'hybrid_key' : 'generated primary key for this table; no other significance',
		'experiment_key' : 'foreign key to mapping_experiment table, identifying the main experiment record',
		'band' : 'region on chromosome to which marker is assigned according to experimental results',
		'concordance_type' : 'concordance by chromosome or by marker?',
		},
	Table.INDEX : {
		'experiment_key' : 'fast lookup of HYBRID data for an experiment',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
