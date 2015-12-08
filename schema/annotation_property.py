#!/usr/local/bin/python

import Table

# contains data definition information for the annotation_inferred_from_id
# table

###--- Globals ---###

# name of this database table
tableName = 'annotation_property'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		NOT NULL,
	annotation_key		int		NOT NULL,
	type			text	NULL,
	property		text	NOT NULL,
	value			text	NOT NULL,
	stanza			int		NOT NULL,
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
	Table.TABLE : 'petal table for the annotation flower, containing annotation properties',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this record',
		'annotation_key' : 'identifies the annotation',
		'type' : 'Custom annotation type',
		'property' : 'property name',
		'value' : 'property value',
		'stanza' : 'Which grouping this property belongs',
		'sequence_num' : 'used for ordering rows for a given annotation',
		},
	Table.INDEX : {
		'annotation_key' : 'clusters data so all iproperties can be quickly retrieved for a given annotation',
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
