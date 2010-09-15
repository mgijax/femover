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
	assay_type		varchar(255)	not null,
	full_coding_assay_type	varchar(80)	not null,
	PRIMARY KEY(assay_type))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'fcAssayType' : 'create index %s on %s (full_coding_assay_type)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
