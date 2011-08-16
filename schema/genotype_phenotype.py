#!/usr/local/bin/python

import Table

# contains data definition information for the genotype_phenotype table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'genotype_phenotype'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	genotype_phenotype_key	int		not null,
	genotype_key		int		not null,
	is_header_term		int		not null,
	term			varchar(255)	null,
	term_id			varchar(40)	null,
	indentation_level	int		not null,
	reference_count		int		not null,
	note_count		int		not null,
	sequence_num		int		not null,
	PRIMARY KEY(genotype_phenotype_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'genotype_key' : 'create index %s on %s (genotype_key, sequence_num)',
	}

keys = { 'genotype_key' : ('genotype', 'genotype_key') }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
