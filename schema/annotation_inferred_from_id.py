#!/usr/local/bin/python

import Table

# contains data definition information for the annotation_inferred_from_id
# table

###--- Globals ---###

# name of this database table
tableName = 'annotation_inferred_from_id'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		NOT NULL,
	annotation_key		int		NOT NULL,
	logical_db		text	NULL,
	acc_id			text	NULL,
	organism		text	NULL,
	preferred		int		NOT NULL,
	private			int		NOT NULL,
	sequence_num		int		NOT NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'annotation_key' : ('annotation', 'annotation_key') }

# index used to cluster data in the table
clusteredIndex = ('annotation_key', 'create index %s on %s (annotation_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the annotation flower, containing IDs from which annotations were inferred.  Can have multiple rows per annotation.',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this record',
		'annotation_key' : 'identifies the annotation',
		'logical_db' : 'entity assigning this ID',
		'acc_id' : 'accession ID used to infer the annotation',
		'preferred' : 'is this the preferred accession ID for the object for this logical_db',
		'private' : 'is this ID to be considered private (1) or public (0)?',
		'sequence_num' : 'used for ordering rows for a given annotation',
		},
	Table.INDEX : {
		'annotation_key' : 'clusters data so all inferred-from IDs can be quickly retrieved for a given annotation',
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
