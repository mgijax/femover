#!/usr/local/bin/python

import Table

# contains data definition information for the expression_index_counts table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_index_counts'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	index_key			int	not null,
	assay_age_count			int	not null,
	fully_coded_assay_count		int	not null,
	fully_coded_result_count	int	not null,
	PRIMARY KEY(index_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
