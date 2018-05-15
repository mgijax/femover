#!/usr/local/bin/python

import Table

# contains data definition information for the strain_to_reference table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain_to_reference'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		NOT NULL,
	strain_key		int		NOT NULL,
	disease_key		int		NOT NULL,
	disease_id		text	NULL,
	disease			text	NULL,
	sequence_num	int		NOT NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'disease_key' : 'create index %s on %s (disease_key)',
	'disease_id' : 'create index %s on %s (disease_id)',
	}

# column name -> (related table, column in related table)
keys = {
	'strain_key' : ('strain', 'strain_key'),
	'disease_key' : ('term', 'term_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('strain_key', 'create index %s on %s (strain_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for strain flower, including each disease associated to a strain via a genotype',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this record, no other purpose',
		'strain_key' : 'identifies the strain',
		'disease_key' : 'identifies the disease, FK to term table',
		'disease_id' : 'primary accession ID of the disease',
		'disease' : 'term of the disease',
		'sequence_num' : 'precomputed integer for ordering diseases of a strain',
		},
	Table.INDEX : {
		'disease_key' : 'look up all strains for a disease by key',
		'disease_id' : 'look up all strains for a disease by ID',
		'strain_key' : 'clusters data so all references for a strain are stored together on disk, to aid quick retrieval',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
