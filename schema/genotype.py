#!/usr/local/bin/python
#
# 10/24/2013    lec
#       - TR11423/added 'exists_as' term to 'genotype' table

import Table

# contains data definition information for the genotype table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'genotype'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	genotype_key		int		not null,
	background_strain	text	null,
	primary_id		text	null,
	is_conditional		int		not null,
	note			text	null,
	combination_1		text	null,
	combination_2		text	null,
	has_image		int		not null,
	has_phenotype_data	int		not null,
	is_disease_model	int		not null,
	genotype_type		text	null,
	cell_lines		text	null,
	exists_as		text		not null,
	PRIMARY KEY(genotype_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'primary_id' : 'create index %s on %s (primary_id)',
	'exists_as' : 'create index %s on %s (exists_as)',
	'genotype_key' : 'create index %s on %s (genotype_key)',
	}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'central table for the genotype flower, containing basic data for each genotype',
	Table.COLUMN : {
		'genotype_key' : 'unique key identifying this genotype, same as _Genotype_key in mgd',
		'background_strain' : 'name of the mouse strain used as a background',
		'primary_id' : 'preferred accession ID for this genotype',
		'is_conditional' : '1 if this is a conditional genotype, 0 if not',
		'note' : 'note for the genotype',
		'combination_1' : 'allele pairs put onto the background strain, format style 1',
		'combination_2' : 'allele pairs put onto the background strain, format style 2',
		'has_image' : '1 if this genotype has at least one displayable image, 0 if not',
		'has_phenotype_data' : '1 if this genotype has associated phenotype annotations, 0 if not',
		'is_disease_model' : '1 if this genotype is a mouse model for a human disaese, 0 if not',
		'genotype_type' : 'type of genotype (homozygous, heterozygous, etc.)',
		'cell_lines' : 'cell lines involved in genotype',
		'exists_as' : 'genotype exists-as this type of mouse',
		},
	Table.INDEX : {
		'primary_id' : 'quick lookup by accession ID',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, {}, comments,
		clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
