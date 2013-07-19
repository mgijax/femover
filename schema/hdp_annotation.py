#!/usr/local/bin/python

import Table

# contains data definition information for the annotation table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
# human disease portal
tableName = 'hdp_annotation'

#
# statement to create this table
#
# hdp (human disease portal)
#	. defines human or mouse markers that are annotated
#	  to a term (OMIM id (human), OMIM id (mouse), MP id)
#
#	. The human/mouse marker may/may not contain a genotype 
#
#	. The gatherer contains rules which include/exclude certain
#	  types of annotations from being included in this table
#
#	. The gatherer contains the list of which vocab_name types 
#	  are included in the table
#
createStatement = '''CREATE TABLE %s  ( 
	hdp_annotation_key	int	not null,
	marker_key		int	not null,
	term_key		int	not null,
	vocab_key		int	not null,
	genotype_key		int	null,
	term			text	not null,
	term_id			text	not null,
	vocab_name		text	not null,
	PRIMARY KEY(hdp_annotation_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'central table for the annotation petal, containing one row for each human disease portal annotation',
	Table.COLUMN : {
		'hdp_annotation_key' : 'unique key identifying this human disease portal annotation',
		'marker_key' : 'marker that is annotated to the term',
		'term_key' : 'term that is annotated to the marker',
		'vocab_key' : 'key of the vocabulary containing the annotated term',
		'genotype_key' : 'genotype that is annotated to the marker (at most one genotype per marker)',
		'term' : 'text of the term itself',
		'term_id' : 'primary accession ID for the term',
		'vocab_name' : 'name of the vocabulary containing the annotated term',
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
