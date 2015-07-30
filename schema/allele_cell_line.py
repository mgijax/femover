#!/usr/local/bin/python

import Table

# contains data definition information for the allele_cell_line table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'allele_cell_line'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	allele_key		int		not null,
	is_parent		int		not null,
	is_mutant		int		not null,
	mgd_cellline_key	int		not null,
	cellline_type		text	not null,
	cellline_name		text	null,
	primary_id		text	null,
	logical_db		text	null,
	background_strain	text	null,
	vector			text	null,
	vector_type		text	null,
	creator			text	null,
	sequence_num		int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'mgd_cellline_key' : 'create index %s on %s (mgd_cellline_key)',
	'primary_id' : 'create index %s on %s (primary_id)',
	}

# column name -> (related table, column in related table)
keys = { 'allele_key' : ('allele', 'allele_key') }

# index used to cluster data in the table
clusteredIndex = ('allele_key', 'create index %s on %s (allele_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'contains parent and mutant cell lines for alleles; not normalized -- the same cell line may be repeated for multiple alleles',
	Table.COLUMN : {
		'unique_key' : 'uniquely identifies each row of the table (needed for Hibernate)',
		'allele_key' : 'foreign key to allele table',
		'is_parent' : '1 if this is a parent cell line for the allele, 0 if not',
		'is_mutant' : '1 if this is a mutant cell line for the allele, 0 if not',
		'mgd_cellline_key' : 'corresponds to ALL_CellLine._CellLine_key in mgd',
		'cellline_type' : 'type of cell line',
		'cellline_name' : 'name of this cell line',
		'primary_id' : 'primary accession ID for this cell line',
		'logical_db' : 'logical database assigning the primary ID',
		'background_strain' : 'name of the background strain',
		'vector' : 'name of the vector',
		'vector_type' : 'type of vector',
		'creator' : 'party responsible for producing this cell line',
		'sequence_num' : 'used to order cell lines for each allele',
		},
	Table.INDEX : {
		'mgd_cellline_key' : 'enables quick lookup of all alleles involving a given _CellLine_key from mgd',
		'primary_id' : 'enables quick lookup of all alleles for a given cell line ID',
		'allele_key' : 'clustered index; ensures efficient retrieval of all cell lines for an allele',
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
