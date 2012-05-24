#!/usr/local/bin/python

import Table

# contains data definition information for the allele_to_image table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'allele_to_image'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	allele_key		int		not null,
	image_key		int		not null,
	qualifier		varchar(80)	null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'image_key' : 'create index %s on %s (image_key)',
	}

keys = {
	'allele_key' : ('allele', 'allele_key'),
	'image_key' : ('image', 'image_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('allele_key',  'create index %s on %s (allele_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'join table between the allele and image flowers',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this relationship',
		'allele_key' : 'identifies the allele',
		'image_key' : 'identifies the image',
		'qualifier' : 'qualifier describing the association',
		},
	Table.INDEX : {
		'allele_key' : 'clusters the data so that all images for an allele are stored together for quick access',
		'image_key' : 'look up alleles for an image',
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
