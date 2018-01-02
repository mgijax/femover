#!/usr/local/bin/python

import Table

# contains data definition information for the annotation_source table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'annotation_source'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key				int		not null,
	annotation_key			int		not null,
	source_annotation_key	int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'source_annotation_key' : 'create index %s on %s (source_annotation_key)'
	}

keys = {
	'annotation_key' : ('annotation', 'annotation_key'),
	'source_annotation_key' : ('annotation', 'annotation_key')
	}

# index used to cluster data in the table
clusteredIndex = ('annotation_key', 'create index %s on %s (annotation_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the annotation flower, containing source annotation key for each derived annotation',
	Table.COLUMN : {
		'unique_key' : 'uniquely identifies this record',
		'annotation_key' : 'identifies the annotation',
		'source_annotation_key' : 'identifies an annotation from which this one was derived',
		},
	Table.INDEX : {
		'annotation_key' : 'clusters the data so all the sources for an annotation may be quickly retrieved',
		'source_annotation_key' : 'ensures we can quickly retrieve all annotations that were derived from a particular one',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
