#!/usr/local/bin/python

import Table

# contains data definition information for the expression_index_assay_type_map
# table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_index_assay_type_map'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	assay_type		text	not null,
	full_coding_assay_type	text	null,
	sequence_num		int		not null,
	PRIMARY KEY(assay_type))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'fcAssayType' : 'create index %s on %s (full_coding_assay_type)',
	}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table in the expression literature flower, containing a mapping from each index-style assay type to its equivalent full-coded assay type.  (from expression_index_stages, for instance)',
	Table.COLUMN : {
		'assay_type' : 'a literature index-style assay type',
		'full_coding_assay_type' : 'its equivalent full-coded assay type',
		'sequence_num' : 'used to order assay types',
		},
	Table.INDEX : {
		'fcAssayType' : 'look up the literature index assay type(s) for a full-coded assay type',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, {}, comments,
		clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
