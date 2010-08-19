#!/usr/local/bin/python

import Table

# contains data definition information for the allele table

###--- Globals ---###

# name of this database table
tableName = 'allele'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	allele_key		int		not null,
	symbol			varchar(60)	null,
	name			varchar(255)	null,
	only_allele_symbol	varchar(60)	null,
	gene_name		varchar(255)	null,
	primary_id		varchar(30)	null,
	logical_db		varchar(80)	null,
	allele_type		varchar(255)	null,
	allele_subtype		varchar(255)	null,
	is_recombinase		int		not null,
	driver			varchar(50)	null,
	inducible_note		varchar(255)	null,
	molecular_description	varchar(2048)	null,
	PRIMARY KEY(allele_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'symbol' : 'create index %s on %s (symbol)',
	'primary_id' : 'create index %s on %s (primary_id)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
