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
	'allele_key' : 'create index %s on %s (allele_key)',
	'grid_row_key' : 'create index %s on %s (grid_row_key)',
	'grid_column_key' : 'create index %s on %s (grid_column_key)',
	}

keys = {
	'grid_row_key' : ('allele_grid_row', 'grid_row_key'),
	'grid_column_key' : ('allele_grid_column', 'grid_column_key'),
	'allele_key' : ('allele', 'allele_key'),
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
