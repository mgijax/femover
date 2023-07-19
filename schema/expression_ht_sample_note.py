#!./python

import Table

# contains data definition information for the expression_ht_sample_note table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_ht_sample_note'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int             not null,
        sample_key              int             not null,
        note_type               text    not null,
        note                    text    null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {
        'sample_key' : ('expression_ht_sample', 'sample_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('sample_key', 'create index %s on %s (sample_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'contains notes for high-throughput expression samples',
        Table.COLUMN : {
                'unique_key' : 'uniquely identifies this database record; no other significance',
                'sample_key' : 'identifies the sample for this note',
                'note_type' : 'type of the note',
                'note' : 'text of the note',
                },
        Table.INDEX : {
                'sample_key' : 'clustered index for grouping all notes of a sample together (for quick access)',
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
