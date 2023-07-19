#!./python

import Table

# contains data definition information for the term table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'term'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        term_key        int             not null,
        term            text    null,
        primary_id      text    null,
        vocab_name      text    not null,
        display_vocab_name text not null,
        abbreviation    text    null,
        definition      text    null,
        sequence_num    int             null,
        is_root         int             not null,
        is_leaf         int             not null,
        is_obsolete     int             not null,
        PRIMARY KEY(term_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'primary_id' : 'create index %s on %s (primary_id)',
        'vocab_name' : 'create index %s on %s (vocab_name, is_root)',
        'term' : 'create index %s on %s (term)',
        }

keys = {}

# index used to cluster data in the table
clusteredIndex = ('vocab_term_key',
        'create index %s on %s (vocab_name, term_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'central table for the term flower; contains basic info about vocabulary terms',
        Table.COLUMN : {
                'term_key' : 'unique key for this vocabulary term',
                'term' : 'text name of this term',
                'primary_id' : 'primary accession ID for this term',
                'vocab_name' : 'name of the vocabulary of which this term is a part',
                'display_vocab_name' : 'what should be displayed as the vocabulary name on the web',
                'definition' : 'text definition for this term',
                'sequence_num' : 'used for ordering vocabulary terms',
                'is_root' : '1 if this is a root node, 0 if not',
                'is_leaf' : '1 if this is a leaf node, 0 if not',
                'is_obsolete' : '1 if this term is obsolete, 0 if not',
                },
        Table.INDEX : {
                'primary_id' : 'lookup by accession ID, case sensitive',
                'vocab_name' : 'quickly find the root terms for a vocabaulary, cases sensitive',
                'term' : 'lookup by term name, case sensitive',
                'vocab_term_key' : 'used to cluster data, so all terms of a vocabulary are near each other on disk',
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
