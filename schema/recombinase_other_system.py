#!/usr/local/bin/python

import Table

# contains data definition information for the recombinase_other_system table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'recombinase_other_system'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	allele_system_key	int		not null,
	allele_id		text	null,
	other_system		text	null,
	other_system_key	int		null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = {
	'allele_system_key' : ('recombinase_allele_system',
		'allele_system_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('allele_system_key',
	'create index %s on %s (allele_system_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the recombinase flower, containing (for each allele/system pair) other systems in which the allele has recombinase activity',
	Table.COLUMN : {
		'unique_key' : 'uniquely identifies this record',
		'allele_system_key' : 'identifies the allele/system pair',
		'allele_id' : 'primary ID of the allele, cached here for convenience',
		'other_system' : 'name of another system in which the allele has shown recombinase activity, cached for convenience',
		'other_system_key' : 'term key for the other system',
		},
	Table.INDEX : {
		'allele_system_key' : 'clusters data so that all other system for an allele/system pair are grouped together on disk to aid efficiency',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments,
		clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
