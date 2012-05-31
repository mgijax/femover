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

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the allele flower, containing pre-computed sorts that can be used to order alleles in different ways',
	Table.COLUMN : {
		'allele_key' : 'identifies the allele',
		'by_symbol' : 'sort by allele symbol',
		'by_allele_type' : 'sort by type of allele',
		'by_primary_id' : 'sort by primary accession ID',
		'by_driver' : 'sort by driver then allele symbol, for recombinase alleles (which are the ones with drivers)',
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
