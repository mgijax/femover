#!/usr/local/bin/python

import Table

# contains data definition information for the genotype_to_image table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'genotype_to_image'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int		not null,
	genotype_key	int		not null,
	image_key	int		not null,
	reference_key	int		null,
	qualifier	varchar(80)	null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'genotype_key' : 'create index %s on %s (genotype_key)',
	'image_key' : 'create index %s on %s (image_key)',
	'reference_key' : 'create index %s on %s (reference_key)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
