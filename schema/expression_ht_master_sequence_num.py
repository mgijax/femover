#!/usr/local/bin/python

import Table

# contains data definition information for the expression_ht_master_sequence_num table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_ht_master_sequence_num'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	integer		not null,
	is_classical	integer		not null,
	result_key	integer		not null,
	by_gene_symbol	integer		not null,
	by_age		integer		not null,
	by_structure	integer		not null,
	by_expressed	integer		not null,
	by_experiment	integer		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
# Skip the indexes, as they're a detriment to performance when populating
# the table via bulk load in post-processing. Add them at the end of the
# post-processing step, if we need them.
	}

# column name -> (related table, column in related table)
keys = {
	# skip foreign keys, as result_key could refer to two different tables
	}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'Contains pre-computed sort values for expression results, including both classical and RNA-Seq experiments',
	Table.COLUMN : {
		'unique_key' : 'primary key for the table; identifies each row, no other practical use',
		'is_classical' : 'integer flag; indicates whether this row is for a classical result (1) or an RNA-Seq measurement (0)',
		'result_key' : 'refers to expression_result_summary.result_key for classical rows or to expression_ht_consolidated_sample_measurement.consolidated_measurement_key for RNA-Seq rows',
		'by_gene_symbol' : 'precomputed integer for sorting rows by gene symbol',
		'by_age' : 'precomputed integer for sorting rows by age',
		'by_structure' : 'precomputed integer for sorting rows by structure',
		'by_expressed' : 'precomputed integer for sorting rows by expressed',
		'by_experiment' : 'precomputed integer for sorting rows by the reference column (which is really the experiment ID + name)',
		},
	Table.INDEX : {},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
