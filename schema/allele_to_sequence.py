#!/usr/local/bin/python

import Table

# contains data definition information for the allele_to_sequence table

###--- Globals ---###

# name of this database table
tableName = 'allele_to_sequence'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int		NOT NULL,
	allele_key	int		NOT NULL,
	sequence_key	int 		NOT NULL,
	reference_key	int		NOT NULL,
	qualifier	text	NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'sequence_key' : 'create index %s on %s (sequence_key, allele_key)',
	'reference_key' : 'create index %s on %s (reference_key)',
	}

keys = {
	'allele_key' : ('allele', 'allele_key'),
	'sequence_key' : ('sequence', 'sequence_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('allele_key',
	'create index %s on %s (allele_key, sequence_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'join table between the allele and sequence flowers',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this record, no other purpose',
		'allele_key' : 'identifies the allele',
		'sequence_key' : 'identifies the sequence',
		'reference_key' : 'identifies the reference',
		'qualifier' : 'qualifier describing this association',
		},
	Table.INDEX : {
		'allele_key' : 'clusters the data to be able to quickly retrieve all sequences for an allele',
		'sequence_key' : 'look up alleles for a given sequence',
		'reference_key' : 'look up sequence/allele associations that come from a given reference',
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
