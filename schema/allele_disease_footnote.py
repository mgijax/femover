#!/usr/local/bin/python

import Table

# contains data definition information for the allele_disease_footnote table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'allele_disease_footnote'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	allele_disease_key	int		not null,
	number			int		not null,
	note			varchar(255)	not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'allele_disease' : 'create index %s on %s (allele_disease_key, number)',
	}

keys = { 'allele_disease_key' : ('allele_disease', 'allele_disease_key') }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
