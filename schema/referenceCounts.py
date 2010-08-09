#!/usr/local/bin/python

import Table

# contains data definition information for the reference_counts table

###--- Globals ---###

# name of this database table
tableName = 'reference_counts'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	reference_key		int	NOT NULL,
	marker_count		int 	NULL,
	probe_count		int	NULL,
	mapping_expt_count	int	NULL,
	gxd_index_count		int	NULL,
	gxd_result_count	int	NULL,
	gxd_structure_count	int	NULL,
	gxd_assay_count		int	NULL,
	allele_count		int	NULL,
	sequence_count		int	NULL,
	PRIMARY KEY(reference_key))''' % tableName

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
