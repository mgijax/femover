#!/usr/local/bin/python

import Table

# contains data definition information for the genotype_disease table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'genotype_disease'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	genotype_disease_key	int		not null,
	allele_genotype_key	int		not null,
	is_heading		int		not null,
	is_not			int		not null,
	term			varchar(255)	null,
	term_id			varchar(40)	null,
	reference_count		int		not null,
	has_footnote		int		not null,
	sequence_num		int		not null,
	PRIMARY KEY(genotype_disease_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'allele_genotype' : 'create index %s on %s (allele_genotype_key, sequence_num)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)