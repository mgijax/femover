#!./python

import Table

# contains data definition information for the template table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'term_default_parent'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key                      int             not null,
        term_key                        int             not null,
        default_parent_key      int             not null,
        edge_label                      text    null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'term_key' : 'create index %s on %s (term_key)',
        'default_parent_key' : 'create index %s on %s (default_parent_key)',
        }

# column name -> (related table, column in related table)
keys = {
        'term_key' : ('term', 'term_key'),
        'default_parent_key' : ('term', 'term_key'),
        }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'stores the key of the default parent for each vocabulary term; eventually this could move into the term table, but keeping it separate for now',
        Table.COLUMN : {
                'unique_key' : 'uniquely identifies this row (primary key)',
                'term_key' : 'identifies the child term',
                'default_parent_key' : 'identifies the default parent of the child term (needed to generate display of tree view)',
                'edge_label' : 'type of edge between child and parent',
                },
        Table.INDEX : {
                'term_key' : 'quick retrieval of default parent for a child term',
                'default_parent_key' : 'quick retrieval of all children that have a certain default parent',
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
