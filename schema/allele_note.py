#!/usr/local/bin/python

import Table

# contains data definition information for the alleleNote table

###--- Globals ---###

# name of this database table
tableName = 'allele_note'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int	NOT NULL,
	allele_key	int	NOT NULL,
	note_type	varchar(255)	NOT NULL,
	note		varchar(64000)	NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'allele_key' : ('allele', 'allele_key') }

# index used to cluster data in the table
clusteredIndex = ('allele_key',
	'create index %s on %s (allele_key, note_type)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table in the allele flower, containing various notes for alleles',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this record, no other purpose (not the same as _Note_key in mgd)',
		'allele_key' : 'identifies the allele',
		'note_type' : 'type of note',
		'note' : 'text of the note itself',
		},
	Table.INDEX : {
		'allele_key' : 'clusters data so that all notes for an allele are stored near each other on disk, to aid quick retrieval by allele',
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
