#!/usr/local/bin/python

import Table

# contains data definition information for the annotation table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
# human disease portal
tableName = 'hdp_marker_to_reference'

#
# statement to create this table
#
# hdp (human disease portal)
#
# A row in this table represents:
#	a marker -> reference association between:
#		a) mouse/OMIM (1005) via a super-simple or simple genotype
#		b) mouse marker/allele/OMIM (1012) (no genotypes)
#
# See gather for more information
#
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int	not null,
	marker_key		int	not null,
	reference_key		int	not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'marker_key' : 'create index %s on %s (marker_key)',
        'reference_key' : 'create index %s on %s (reference_key)',
        }

keys = {
        'marker_key' : ('marker', 'marker_key'),
        'reference_key' : ('reference', 'reference_key'),
        }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'central table for the marker/reference petal, containing one row for each marker/reference',
	Table.COLUMN : {
		'unique_key' : 'unique key for this record',
		'marker_key' : 'mouse marker that is annotated to OMIM/disease term',
		'reference_key' : 'reference of the annotation via the mouse marker',
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
