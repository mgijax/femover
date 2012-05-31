#!/usr/local/bin/python

import Table

# contains data definition information for the allele_grid_value table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'allele_grid_value'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	grid_value_key		int		not null,
	grid_row_key		int		not null,
	grid_column_key		int		not null,
	allele_key		int		not null,
	value			varchar(8)	not null,
	PRIMARY KEY(grid_value_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'grid_row_key' : 'create index %s on %s (grid_row_key)',
	'grid_column_key' : 'create index %s on %s (grid_column_key)',
	}

keys = {
	'grid_row_key' : ('allele_grid_row', 'grid_row_key'),
	'grid_column_key' : ('allele_grid_column', 'grid_column_key'),
	'allele_key' : ('allele', 'allele_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('allele_key', 'create index %s on %s (allele_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table in the allele flower, containing the values that go in the phenotype grid for each allele',
	Table.COLUMN : {
		'grid_value_key' : 'unique identifier for this row, no other purpose',
		'grid_row_key' : 'identifies the row',
		'grid_column_key' : 'identifies the column',
		'allele_key' : 'identifies the allele',
		'value' : 'value to go in the grid cell at (row, column) for this allele',
		},
	Table.INDEX : {
		'allele_key' : 'clusters the data so all records for an allele will be together on disk, for quick access',
		'grid_row_key' : 'look up values for a row',
		'grid_column_key' : 'look up values for a column',
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
