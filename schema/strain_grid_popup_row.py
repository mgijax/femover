#!/usr/local/bin/python

import Table

# contains data definition information for the strain_grid_popup_row table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain_grid_popup_row'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	row_key			int		not null,
	grid_cell_key	int		not null,
	genotype_key	int		not null,
	sequence_num	int		not null,
	PRIMARY KEY(row_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = { 'genotype_key' : 'create index %s on %s (genotype_key)' }

# column name -> (related table, column in related table)
keys = {
	'grid_cell_key' : ('strain_grid_cell', 'grid_cell_key'),
	'genotype_key' : ('genotype', 'genotype_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('grid_cell_key', 'create index %s on %s (grid_cell_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'defines rows for the table in the strain phenotype popup, one row per genotype with annotations in that cell in the strain phenogrid',
	Table.COLUMN : {
		'row_key' : 'unique key for this table; identifies one row (with annotations for a single genotype)',
		'grid_cell_key' : 'identifies which phenogrid cell was clicked to find this row (on a strain detail page)',
		'genotype_key' : 'identifies the genotype displayed in this row',
		'sequence_num' : 'sequence number for ordering the genotype rows to be displayed for a single phenogrid cell',
		},
	Table.INDEX : {
		'genotype_key' : 'quick retrieval for all rows of a given genotype (should be only one, until we branch out beyond phenotype data)',
		'grid_cell_key' : 'clustered index, bringing all genotype rows of a phenogrid cell together for fast access',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
