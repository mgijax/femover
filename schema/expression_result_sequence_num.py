#!/usr/local/bin/python

import Table

# contains data definition information for the expression_result_sequence_num
# table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_result_sequence_num'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	result_key		int		not null,
	by_assay_type		int		not null,
	by_gene_symbol		int		not null,
	by_anatomical_system	int		not null,
	by_age			int		not null,
	by_structure		int		not null,
	by_expressed		int		not null,
	by_mutant_alleles	int		not null,
	by_reference		int		not null,
	PRIMARY KEY(result_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {
	'result_key' : ('expression_result_summary', 'result_key'),
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
