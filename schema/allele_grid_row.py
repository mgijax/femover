#!/usr/local/bin/python

import Table

# contains data definition information for the allele_grid_row table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'allele_grid_row'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	grid_row_key		int		not null,
	allele_key		int		not null,
	term			varchar(255)	null,
	term_id			varchar(40)	null,
	is_header		int		not null,
	indentation_level	int		not null,
	sequence_num		int		not null,
	PRIMARY KEY(grid_row_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'allele_key' : ('allele', 'allele_key') }

# index used to cluster data in the table
clusteredIndex = ('allele_key',
	'create index %s on %s (allele_key, sequence_num)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the allele flower, containing data about the rows of the phenotype grid for each allele',
	Table.COLUMN : {
		'grid_row_key' : 'unique identifier for this record, no other purpose',
		'allele_key' : 'identifies the allele',
		'term' : 'vocabulary term for this row',
		'term_id' : 'primary accession ID for the term',
		'is_header' : '1 if this is a header term, 0 if it is not',
		'indentation_level' : 'number of steps to indent this term',
		'sequence_num' : 'orders the rows for a given allele',
		},
	Table.INDEX : {
		'allele_key' : 'clusters the data so that all rows for an allele are near each other on disk, to aid quick retrieval',
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
