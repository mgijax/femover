#!./python

import Table

# contains data definition information for the term_note table

###--- Globals ---###

# name of this database table
tableName = 'term_note'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int             NOT NULL,
        term_key        int             NOT NULL,
        note_type       text    NOT NULL,
        note            text    NULL,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'term_key' : ('term', 'term_key'), }

# index used to cluster data in the table
clusteredIndex = ('term_key', 'create index %s on %s (term_key, note_type)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table in the term flower, containing various types of textual notes for terms',
        Table.COLUMN : {
                'unique_key' : 'unique key identifying this record, no other purpose',
                'term_key' : 'identifies the term',
                'note_type' : 'type of note',
                'note' : 'text of the note itself',
                },
        Table.INDEX : {
                'term_key' : 'clusters the data so that all notes for a term reside near each other on disk, to aid quick retrieval',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
