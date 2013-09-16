#!/usr/local/bin/python

import Table

# contains data definition information for the annotation table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
# human disease portal
tableName = 'hdp_annotation_to_reference'

#
# statement to create this table
#
# hdp (human disease portal)
#
# A row in this table represents:
#	 a marker -> reference association for the given annotation
#
# See gather for more information
#
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int	not null,
	hdp_annotation_key	int	not null,
	marker_key		int	not null,
	reference_key		int	not null,
	PRIMARY KEY(hdp_annotation_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'hdp_annotation_key' : 'create index %s on %s (hdp_annotatioin_key)',
        'marker_key' : 'create index %s on %s (marker_key)',
        'reference_key' : 'create index %s on %s (reference_key)',
        }

keys = {
        'hdp_annotation_key' : ('hdp_annotation', 'hdp_annotation_key'),
        'marker_key' : ('marker', 'marker_key'),
        'reference_key' : ('reference', 'reference_key'),
        }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'central table for the annotation petal, containing one row for each human disease portal annotation',
	Table.COLUMN : {
		'unique_key' : 'unique key for this record',
		'hdp_annotation_key' : 'unique key identifying this human disease portal annotation',
		'marker_key' : 'marker that is annotated to the term',
		'reference_key' : 'reference of annotation/marker',
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
