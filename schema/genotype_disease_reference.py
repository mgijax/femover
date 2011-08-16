#!/usr/local/bin/python

import Table

# contains data definition info for the genotype_disease_reference table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'genotype_disease_reference'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	genotype_disease_key	int		not null,
	reference_key		int		not null,
	jnum_id			varchar(40)	not null,
	sequence_num		int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'genotype_disease_key' : \
		'create index %s on %s (genotype_disease_key, sequence_num)',
	}

keys = {
	'genotype_disease_key' : ('genotype_disease', 'genotype_disease_key'),
	'reference_key' : ('reference', 'reference_key')
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
