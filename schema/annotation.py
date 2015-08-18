#!/usr/local/bin/python

import Table

# contains data definition information for the annotation table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'annotation'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	annotation_key		int		not null,
	dag_name		text	null,
	qualifier		text	null,
	vocab_name		text	null,
	term			text	null,
	term_id			text	null,
	evidence_code		text	null,
	evidence_term		text	null,
	object_type		text	null,
	annotation_type		text	null,
	reference_count		int		not null,
	inferred_id_count	int		not null,
	PRIMARY KEY(annotation_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'central table for the annotation petal, containing one row for each annotation',
	Table.COLUMN : {
		'annotation_key' : 'unique key identifying this annotation (does not correspond to _Annot_key in mgd)',
		'dag_name' : 'name of the DAG containing the annotated term',
		'qualifier' : 'qualifier describing this annotation',
		'vocab_name' : 'name of the vocabulary containing the annotated term',
		'term' : 'text of the term itself',
		'term_id' : 'primary accession ID for the term',
		'evidence_code' : 'type of evidence supporting the annotation',
		'object_type' : 'type of object annotated to the term',
		'annotation_type' : 'type of annotation',
		'reference_count' : 'count of references for the annotation',
		'inferred_id_count' : 'count of IDs from which this annotation was inferred',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, {}, comments,
		clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
