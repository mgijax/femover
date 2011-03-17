#!/usr/local/bin/python

import Table

# contains data definition information for the image_alleles table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'image_alleles'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	image_key		int		not null,
	allele_key		int		not null,
	allele_symbol		varchar(60)	null,
	allele_name		varchar(512)	null,
	allele_id		varchar(30)	null,
	sequence_num		int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'image_key' : 'create index %s on %s (image_key)',
	'allele_key' : 'create index %s on %s (allele_key)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
