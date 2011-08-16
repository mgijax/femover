#!/usr/local/bin/python

import Table

# contains data definition information for the allele_imsr_counts table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'allele_imsr_counts'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	allele_key		int	not null,
	cell_line_count		int	not null,
	strain_count		int	not null,
	count_for_marker	int	not null,
	PRIMARY KEY(allele_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'allele_key' : ('allele', 'allele_key') }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
