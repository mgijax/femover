#!/usr/local/bin/python

import Table

# contains data definition information for the annotation_sequence_num table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'annotation_sequence_num'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	annotation_key		int	not null,
	by_dag_structure	int	not null,
	by_term_alpha		int	not null,
	by_vocab		int	not null,
	by_annotation_type	int	not null,
	by_vocab_dag_term	int	not null,
	by_marker_dag_term	int	not null,
	PRIMARY KEY(annotation_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'annotation_key' : ('annotation', 'annotation_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the annotation flower, containing pre-computed values for easily ordering annotations.  Contains one record per record in annotation.',
	Table.COLUMN : {
		'annotation_key' : 'identifies the annotation',
		'by_dag_structure' : 'depth-first ordering of DAGs, which groups child terms under their parent terms',
		'by_term_alpha' : 'alphabetical ordering',
		'by_vocab' : 'sort by vocabulary name',
		'by_annotation_type' : 'sort by type of annotation',
		'by_vocab_dag_term' : 'sort by vocabulary, then by DAG, then by term',
		'by_marker_dag_term' : 'sort by marker nomenclature, then by DAG, then by term',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments,
		clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
