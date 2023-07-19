#!./python

import Table

# contains data definition information for the term_annotation_counts table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'term_annotation_counts'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key                              int             not null,
        term_key                                int             not null,
        annotated_object_type                   text    not null,
        object_count_to_term                    int             not null,
        object_count_with_descendents           int             not null,
        annot_count_to_term                     int             not null,
        annot_count_with_descendents            int             not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        }

keys = {
        'term_key' : ('term', 'term_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('term_key', 'create index %s on %s (term_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'caches counts of annotations of various types for this term; can have more than one row per term',
        Table.COLUMN : {
                'unique_key' : 'unique key for this term / object type pair',
                'term_key' : 'foreign key to term table, identifying the term for which these counts are stored',
                'annotated_object_type' : 'type of annotated object (and its annotations) that was counted',
                'object_count_to_term' : 'number of objects annotated to just this term',
                'object_count_with_descendents' : 'number of objects annotated to this term and its descendent terms',
                'annot_count_to_term' : 'count of annotations to just this term',
                'annot_count_with_descendents' : 'count of annotations to this term and its descendents',
                },
        Table.INDEX : {
                'term_key' : 'cluster rows by term_key, to ensure multiple counts for a given term are stored together on disk',
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
