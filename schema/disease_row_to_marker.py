#!/usr/local/bin/python

import Table

# contains data definition information for the disease_row_to_marker table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'disease_row_to_marker'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int		not null,
	disease_row_key	int		not null,
	marker_key	int		not null,
	sequence_num	int		not null,
	is_causative	int		not null,
	organism	varchar(50)	not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'marker_key' : 'create index %s on %s (marker_key)',
	}

# column name -> (related table, column in related table)
keys = { 'marker_key' : ('marker', 'marker_key'),
	'disease_row_key' : ('disease_row', 'disease_row_key')
	}

# index used to cluster data in the table
clusteredIndex = ('disease_row_key',
	'create index %s on %s (disease_row_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'join table between a disease row and the markers it includes',
	Table.COLUMN : {
		'unique_key' : 'uniquely identifies a disease row/marker pair',
		'disease_row_key' : 'foreign key to disease_row table, identifying the row for this marker',
		'marker_key' : 'foreign key to marker table, identifying a marker for this disease row',
		'sequence_num' : 'orders the markers in a given disease row',
		'is_causative' : '1 if this marker causes the disease, 0 if not',
		'organism' : 'organism of the marker, cached here for convenience',
		},
	Table.INDEX : {
		'marker_key' : 'allows retrieval of the disease rows for a given marker (likely seldom used)',
		'disease_row_key' : 'clustered index, used to group together the rows for a given disease row for speed of access',
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
