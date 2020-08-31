#!./python

import Table

# contains data definition information for the marker_qtl_experiments table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_qtl_experiments'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int             not null,
        marker_key      int             not null,
        mgd_expt_key    int             not null,
        jnum_id         text    null,
        note            text            null,
        ref_note        text        null,
        note_type       text    not null,
        sequence_num    int             not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = { 'marker_key' : ('marker', 'marker_key') }

# index used to cluster data in the table
clusteredIndex = ('marker_key', 'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'stores notes and references about mapping experiments for QTL markers',
        Table.COLUMN : {
                'unique_key' : 'uniquely idenfies a record in this table',
                'marker_key' : 'foreign key to marker table',
                'mgd_expt_key' : 'identifies the experiment key from the mgd database',
                'jnum_id' : 'J: number for the reference of this experiment',
                'note' : 'notes from this experiment',
                'ref_note': 'Note from MLD_Notes associated with the reference',
                'sequence_num' : 'used to order records for each marker',
                },
        Table.INDEX : {
                'marker_key' : 'clustered index to ensure all records for a marker are stored together',
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
