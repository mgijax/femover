#!/usr/local/bin/python

import Table

# contains data definition information for the alleleSequenceNum table

###--- Globals ---###

# name of this database table
tableName = 'allele_sequence_num'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	allele_key	int		not null,
	by_symbol	int		not null,
	by_allele_type	int		not null,
	by_primary_id	int		not null,
	by_driver	int		not null,
	PRIMARY KEY(allele_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'allele_key' : ('allele', 'allele_key') }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
