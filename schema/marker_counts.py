#!/usr/local/bin/python

import Table

# contains data definition information for the markerCounts table

###--- Globals ---###

# name of this database table
tableName = 'marker_counts'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	marker_key	int	NOT NULL,
	reference_count	int	NULL,
	sequence_count	int	NULL,
	allele_count	int	NULL,
	go_term_count	int	NULL,
	gxd_assay_count	int	NULL,
	ortholog_count	int	NULL,
	gene_trap_count	int	NULL,
	PRIMARY KEY(marker_key))''' % tableName

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
