#!/usr/local/bin/python

import Table

# contains data definition information for the recombinase_other_allele table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'recombinase_other_allele'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	allele_system_key	int		not null,
	other_allele_key	int		not null,
	other_allele_id		varchar(30)	null,
	other_allele_symbol	varchar(60)	null,
	system_key		int		null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'allele_system_key' : 'create index %s on %s (allele_system_key)',
	}

keys = {
	'allele_system_key' : ('recombinase_allele_system',
		'allele_system_key'),
	'other_allele_key' : ('allele', 'allele_key'),
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
