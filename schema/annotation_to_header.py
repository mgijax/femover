#!./python

import Table

# contains data definition information for the annotation_to_header table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'annotation_to_header'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int     not null,
        annotation_key  int     not null,
        header_term_key int     not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'annotation_key' : 'create index %s on %s (annotation_key)',
        'header_term_key' : 'create index %s on %s (header_term_key)',
        }

# column name -> (related table, column in related table)
keys = {
        'annotation_key' : ('annotation', 'annotation_key'),
        'header_term_key' : ('term', 'term_key'),
        }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'maps from an annotation key for an annotation to the keys of the header terms which are ancestors of the annotated term (can be more than one row per annotation, if multiple headers are reachable)',
        Table.COLUMN : {
                'unique_key' : 'uniquely identifies one row in this table',
                'annotation_key' : 'key of the annotation',
                'header_term_key' : 'key of one of the slim terms (ancestor of the annotated term)',
                },
        Table.INDEX : {
                'annotation_key' : 'quick access to headers for an annotation',
                'header_term_key' : 'look up all annotations under a given header term',
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
