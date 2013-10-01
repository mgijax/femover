#!/usr/local/bin/python

import Table

# contains data definition information for the grid-cluster-annotations table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
# human disease portal
tableName = 'hdp_gridcluster_annotation'

#
# statement to create this table
#
# hdp (human disease portal)
#
# This table represents the "grid cluster" -> annotations (OMIM/MP) relationships
# of the homologene clusters for hdp-specific
# super-simple genotypes (or null genotypes).
#
# plus it adds the mouse/human annotations that do NOT exist in the homologene cluster
#
# see the gatherer for more information
#
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int	not null,
	hdp_gridcluster_key	int	not null,
	term_key		int 	not null,
        annotation_type         int     not null,
        term_type         	text    not null,
	term_id			text	not null,
	term			text	not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'hdp_gridcluster_key' : 'create index %s on %s (hdp_gridcluster_key)',
        'term_key' : 'create index %s on %s (term_key)',
        'annotation_type' : 'create index %s on %s (annotation_type)',
        'term_type' : 'create index %s on %s (term_type)',
        'term_id' : 'create index %s on %s (term_id)',
        }

keys = {
        'hdp_gridcluster_key' : ('hdp_gridcluster', 'hdp_gridcluster_key'),
        'term_key' : ('term', 'term_key'),
        }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the grid cluster flower containing data about annotations that belong to a given grid cluster',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this record, no other purpose',
		'hdp_gridcluster_key' : 'unique key identifying this human disease portal grid cluster',
                'term_key' : 'term that is annotated to the marker',
                'annotation_type' : 'type of annotation',
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