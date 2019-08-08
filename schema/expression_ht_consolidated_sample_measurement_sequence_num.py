#!/usr/local/bin/python

import Table

# contains data definition information for the expression_ht_consolidated_sample_measurement table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_ht_consolidated_sample_measurement_sequence_num'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	consolidated_measurement_key	integer				not null,
	by_gene_symbol					integer				not null,
	by_age							integer				not null,
	by_structure					integer				not null,
	by_expressed					integer				not null,
	by_experiment					integer				not null,
	PRIMARY KEY(consolidated_measurement_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
# Skip these indexes, as they're a detriment to performance when populating
# the table in post-processing. Add them at the end of the post-processing
# step, if we need them.  (We probably don't, as we're serving paginated data
# from Solr rather than the database.)
#	'symbol' : 'create index %s on %s (by_gene_symbol)',
#	'age' : 'create index %s on %s (by_age)',
#	'structure' : 'create index %s on %s (by_structure)',
#	'expressed' : 'create index %s on %s (by_expressed)',
#	'experiment' : 'create index %s on %s (by_experiment)',
	}

# column name -> (related table, column in related table)
keys = {
	'consolidated_measurement_key' : ('expression_ht_consolidated_sample_measurement', 'consolidated_measurement_key'),
	}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'Contains pre-computed sort values for measurements of consolidated samples',
	Table.COLUMN : {
		'consolidated_measurement_key' : 'primary key for this table; identifies the "combined sample", the set of samples that are biological replicates',
		'by_gene_symbol' : 'precomputed integer for sorting rows by gene symbol',
		'by_age' : 'precomputed integer for sorting rows by age',
		'by_structure' : 'precomputed integer for sorting rows by structure',
		'by_expressed' : 'precomputed integer for sorting rows by expressed',
		'by_experiment' : 'precomputed integer for sorting rows by the reference column (which is really the experiment ID + name)',
		},
	Table.INDEX : {
#		'symbol' : 'precomputed integers for sorting by gene symbol',
#		'age' : 'precomputed integers for sorting by age',
#		'structure' : 'precomputed integers for sorting by structure',
#		'expressed' : 'precomputed integers for sorting by expressed',
#		'experiment' : 'precomputed integers for sorting by experiment',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
