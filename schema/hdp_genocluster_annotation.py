#!/usr/local/bin/python

import Table

# contains data definition information for the grid-cluster-annotations table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
# human disease portal
tableName = 'hdp_genocluster_annotation'

#
# statement to create this table
#
# hdp (human disease portal)
#
# This table represents the cluster -> annotations (OMIM/MP) relationships
# of the compressed genotype clusters for hdp-specific
# super-simple genotypes
#
# see the gatherer for more information
#
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int	not null,
	hdp_genocluster_key	int	not null,
	term_key		int 	null,
        annotation_type         int     not null,
	qualifier_type          text    null,
        term_type         	text    not null,
	term_id			text	null,
	term			text	not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'hdp_genocluster_key' : 'create index %s on %s (hdp_genocluster_key)',
        'term_key' : 'create index %s on %s (term_key)',
        'annotation_type' : 'create index %s on %s (annotation_type)',
        'qualifier_type' : 'create index %s on %s (qualifier_type)',
        'term_type' : 'create index %s on %s (term_type)',
        'term_id' : 'create index %s on %s (term_id)',
        }

keys = {
        'hdp_genocluster_key' : ('hdp_genocluster', 'hdp_genocluster_key'),
        'term_key' : ('term', 'term_key'),
        }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the grid cluster flower containing data about annotations that belong to a given grid cluster',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this record, no other purpose',
		'hdp_genocluster_key' : 'unique key identifying this human disease portal geno cluster',
                'term_key' : 'term that is annotated to the marker',
                'annotation_type' : 'type of annotation',
                'qualifier_type' : 'type of qualifier ("normal", null)',
                'term_type' : 'type of term (term, header)',
                'term_id' : 'primary accession ID for the term',
                'term' : 'text of the term itself',
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
