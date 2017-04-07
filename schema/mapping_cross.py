#!/usr/local/bin/python

import Table

# contains data definition information for the mapping_cross table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'mapping_cross'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	cross_key			int		not null,
	experiment_key		int		not null,
	cross_type			text	not null,
	female_parent		text	null,
	female_strain		text	not null,
	male_parent			text	null,
	male_strain			text	not null,
	panel_name			text	null,
	panel_filename		text	null,
	homozygous_allele	text	null,
	homozygous_strain	text	not null,
	heterozygous_allele	text	null,
	heterozygous_strain	text	not null,
	PRIMARY KEY(cross_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'experiment_key' : 'create index %s on %s (experiment_key)',
	}

# column name -> (related table, column in related table)
keys = {
	'experiment_key' : ('mapping_experiment', 'experiment_key'),
	}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'contains additional data for CROSS experiments, augmenting basic data in mapping_experiment table',
	Table.COLUMN : {
		'cross_key' : 'generated primary key for this table; no other significance',
		'experiment_key' : 'foreign key to mapping_experiment table, identifying the experiment',
		'cross_type' : 'more specific type of CROSS experiment',
		'female_parent' : 'genotype of mother',
		'female_strain' : 'strain of mother',
		'male_parent' : 'genotype of father',
		'male_strain' : 'strain of father',
		'panel_name' : 'name of the cross',
		'panel_filename' : 'name of file with cross data',
		'homozygous_allele' : 'strain abbreviation for homozygous strain',
		'homozygous_strain' : 'strain name for homozygous strain',
		'heterozygous_allele' : 'strain abbreviation for heterozygous strain',
		'heterozygous_strain' : 'strain name for heterozygous strain',
		},
	Table.INDEX : {
		'experiment_key' : 'quick lookup of CROSS data by experiment key',
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
