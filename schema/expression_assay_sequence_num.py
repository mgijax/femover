#!/usr/local/bin/python

import Table

# contains data definition information for the expression_assay_sequence_num
# table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_assay_sequence_num'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	assay_key		int		not null,
	by_symbol		int		not null,
	by_assay_type		int		not null,
	by_citation		int		not null,
	PRIMARY KEY(assay_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = {
	'assay_key' : ('expression_assay', 'assay_key'),
	}

comments = {
	Table.TABLE : 'petal table for expression_assay, storing pre-computed sorts for assays',
	Table.COLUMN : {
	    'assay_key' : 'uniquely identifies the expression assay',
	    'by_symbol' : 'pre-computed sort by gene symbol, assay type, and mini citation',
	    'by_assay_type' : 'pre-computed sort by assay type, gene symbol, and mini citation',
	    'by_citation' : 'pre-computed sort by mini citation, gene symbol, and assay type',
	    },
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
