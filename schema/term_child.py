#!./python

import Table

# contains data definition information for the term_child table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'term_child'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int             not null,
        term_key                int             not null,
        child_term_key          int             not null,
        child_term              text    null,
        child_primary_id        text    null,
        sequence_num            int             null,
        is_leaf                 int             not null,
        edge_label              text    null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'term_key' : 'create index %s on %s (term_key)',
        'child_term_key' : 'create index %s on %s (child_term_key)',
        }

keys = {
        'term_key' : ('term', 'term_key'),
        'child_term_key' : ('term', 'term_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('term_key_seqnum',
        'create index %s on %s (term_key, sequence_num)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'contains one row for each child of each term',
        Table.COLUMN : {
                'unique_key' : 'unique key for this term / child pair',
                'term_key' : 'foreign key to term table, identifying the term we are considering',
                'child_term_key' : 'foreign key to term table, identifying the child term',
                'child_term' : 'name for the child term, cached for convenience',
                'child_primary_id' : 'accession ID for the child term, cached for convenience',
                'sequence_num' : 'sequence number for ordering children of a givne term',
                'is_leaf' : '1 if this child term is a leaf node, 0 if not',
                'edge_label' : 'type of edge between the term and its child',
                },
        Table.INDEX : {
                'term_key' : 'quick access to children of a given term',
                'term_key_seqnum' : 'cluster data by term key and sequence number, so children will be ready to go',
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
