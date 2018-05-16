#!/usr/local/bin/python

import Table

# contains data definition information for the strain_disease_to_genotype table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain_disease_to_genotype'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key				int		NOT NULL,
	strain_disease_key		int		NOT NULL,
	strain_genotype_key		int		NOT NULL,
	qualifier				text	NULL,
	sequence_num			int		NOT NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'strain_genotype_key' : 'create index %s on %s (strain_genotype_key)',
	}

# column name -> (related table, column in related table)
keys = {
	'strain_disease_key' : ('strain_disease', 'strain_disease_key'),
	'strain_genotype_key' : ('strain_genotype', 'strain_genotype_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('strain_disease_key', 'create index %s on %s (strain_disease_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'join table between strain_disease and strain_genotype, identifying which diseases have annotations for which genotypes for each strain',
	Table.COLUMN : {
		'unique_key' : 'generated key to uniquely identify each record, no other purpose',
		'strain_genotype_key' : 'identifies the strain/genotype pair',
		'strain_disease_key' : 'identifies the strain/disease pair',
		'qualifier' : 'qualifier for the annotation (null or NOT) for the disease/genotype pair',
		'sequence_num' : 'precomputed integer for ordering genotypes of each disease',
		},
	Table.INDEX : {
		'strain_genotype_key' : 'look up all diseases for a given strain/genotype pair',
		'strain_disease_key' : 'clusters data so all genotypes for a strain/disease pair are stored together on disk, to aid quick retrieval',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
