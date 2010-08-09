#!/usr/local/bin/python

import Table

# contains data definition information for the sequence_gene_model table

###--- Globals ---###

# name of this database table
tableName = 'sequence_gene_model'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	sequence_key		int		NOT NULL,
	marker_type		varchar(80)	NULL,
	biotype			varchar(255)	NULL,
	exon_count		int		NULL,
	transcript_count	int		NULL,
	PRIMARY KEY(sequence_key))''' % tableName

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
