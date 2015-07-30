#!/usr/local/bin/python

import Table

# contains data definition information for the marker_to_phenotype_image table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_to_phenotype_image'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int		not null,
	marker_key	int		not null,
	image_key	int		not null,
	reference_key	int		null,
	qualifier	text	null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'image_key' : 'create index %s on %s (image_key)',
	'reference_key' : 'create index %s on %s (reference_key)',
	}

keys = {
	'marker_key' : ('marker', 'marker_key'),
	'image_key' : ('image', 'image_key'),
	'reference_key' : ('reference', 'reference_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('marker_key', 'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'join table between markers and phenotype images for their alleles',
	Table.COLUMN : {
		'unique_key' : 'uniquely identifies this record',
		'marker_key' : 'identifies the marker',
		'image_key' : 'identifies an image',
		'reference_key' : 'reference supporting this relationship',
		'qualifier' : 'qualifier describing this relationship',
		},
	Table.INDEX : {
		'marker_key' : 'clusters the data so images for a marker are stored together on disk for quick access',
		'image_key' : 'look up markers involved in an image',
		'reference_key' : 'look up all marker/image pairs for a reference',
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
