#!/usr/local/bin/python

import Table

# contains data definition information for the marker_tissue_expression_counts
# table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_tissue_expression_counts'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	marker_key		int		not null,
	structure_key		int		not null,
	structure		text	not null,
	all_result_count	int		not null,
	detected_count		int		not null,
	not_detected_count	int		not null,
	sequence_num		int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'marker_key' : ('marker', 'marker_key'), }

# index used to cluster data in the table
clusteredIndex = ('marker_key', 'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the marker flower, containing expression counts for each structure in which expression was studied in that marker',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this record; no particular significance',
		'marker_key' : 'identifies the marker',
		'structure_key' : 'identifies the structure (This is an mgd-style _Structure_key.)',
		'structure' : 'printName of the structure (its structure name plus its context)',
		'all_result_count' : 'count of all expression results',
		'detected_count' : 'count of results where expression was detected',
		'not_detected_count' : 'count of results where expression was not detected',
		'sequence_num' : 'used to order structures for a marker',
		},
	Table.INDEX : {
		'marker_key' : 'clusters rows together on disk so that all counts for a marker can be retrieved quickly',
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
