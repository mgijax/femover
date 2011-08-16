#!/usr/local/bin/python

import Table

# contains data definition information for the recombinase_allele_system table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'recombinase_allele_system'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	allele_system_key	int		not null,
	allele_key		int		not null,
	allele_id		varchar(30)	null,
	system			varchar(255)	null,
	system_key		int		null,
	PRIMARY KEY(allele_system_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'allele_key' : 'create index %s on %s (allele_key)',
	'allele_id' : 'create index %s on %s (allele_id)',
	}

keys = { 'allele_key' : ('allele', 'allele_key'), }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
