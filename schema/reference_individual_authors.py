#!./python

import Table

# contains data definition information for the reference_individual_authors
# table

###--- Globals ---###

# name of this database table
tableName = 'reference_individual_authors'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int NOT NULL,
        reference_key   int NOT NULL,
        author          text NULL,
        sequence_num    int NOT NULL,
        is_last         int NOT NULL,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'sequence_num' : 'create index %s on %s (sequence_num)',
        'is_last' : 'create index %s on %s (is_last)',
        'author' : 'create index %s on %s (author)',
        }

keys = { 'reference_key' : ('reference', 'reference_key') }

# index used to cluster data in the table
clusteredIndex = ('reference_key', 'create index %s on %s (reference_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the reference flower, containing one record for each individual author name for each reference',
        Table.COLUMN : {
                'unique_key' : 'uniquely identifies this record, no other purpose',
                'reference_key' : 'identifies the reference',
                'author' : 'name of one author for this reference',
                'sequence_num' : 'integer used to order the authors for each reference',
                'is_last' : '1 if this is the last author name for this reference, 0 if not',
                },
        Table.INDEX : {
                'reference_key' : 'used to cluster the data, to bring authors for each reference close together on disk (for quick access)',
                'sequence_num' : 'provides quick access to all first authors (sequence_num = 1)',
                'is_last' : 'provides quick access to all last authors (is_last = 1)',
                'author' : 'lookup by author name',
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
