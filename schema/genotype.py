#!/usr/local/bin/python

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
	background_strain	varchar(255)	null,
	primary_id		varchar(30)	null,
	is_conditional		int		not null,
	note			varchar(255)	null,
	combination_1		varchar(1024)	null,
	combination_2		varchar(1024)	null,
	PRIMARY KEY(genotype_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'primary_id' : 'create index %s on %s (primary_id)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
