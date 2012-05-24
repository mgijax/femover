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
	'image_key' : 'create index %s on %s (image_key)',
	'reference_key' : 'create index %s on %s (reference_key)',
	}

keys = {
	'genotype_key' : ('genotype', 'genotype_key'),
	'image_key' : ('image', 'image_key'),
	'reference_key' : ('reference', 'reference_key')
	}

# index used to cluster data in the table
clusteredIndex = ('genotype_key', 'create index %s on %s (genotype_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'join table between the genotype and image flowers',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this genotype/image/reference trio',
		'genotype_key' : 'identifies the genotype',
		'image_key' : 'identifies the image',
		'reference_key' : 'identifies the reference establishing the relationship',
		'qualifier' : 'qualifier describing the relationship',
		},
	Table.INDEX : {
		'genotype_key' : 'clusters the data so all images for a genotype will be stored together for quick access',
		'image_key' : 'for getting genotypes for an image',
		'reference_key' : 'for getting all genotype/image pairs supported by a particular reference',
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
