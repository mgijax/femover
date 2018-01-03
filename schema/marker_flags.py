#!/usr/local/bin/python

import Table

# contains data definition information for the marker_flags table.  Table has exactly one row per marker
# in the main 'marker' table, so can always use an inner join.

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_flags'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	marker_key							int		not null,
	has_wildtype_expression_data		int		not null,
	has_phenotypes_related_to_anatomy	int		not null,
	PRIMARY KEY(marker_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {
	'marker_key' : ('marker', 'marker_key'),
	}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'contains precomputed flags for markers, especially for data where we do not need or want counts',
	Table.COLUMN : {
		'marker_key' : 'foreign key to marker table, identifying the marker for this row',
		'has_wildtype_expression_data' : 'does this marker have wild-type expression data (1) or not (0)?',
		'has_phenotypes_related_to_anatomy' : 'does this marker have phenotypes related to anatomy terms (1) or not (0)?',
		},
	Table.INDEX : {},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments,
		clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
