#!./python

import Table

# contains data definition information for the annotation_reference table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'annotation_reference'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int             not null,
        annotation_key          int             not null,
        reference_key           int             not null,
        jnum_id                 text    null,
        sequence_num            int             not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = {
        'annotation_key' : ('annotation', 'annotation_key'),
        'reference_key' : ('reference', 'reference_key')
        }

# index used to cluster data in the table
clusteredIndex = ('annotation_key', 'create index %s on %s (annotation_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the annotation flower, containing references for each annotation',
        Table.COLUMN : {
                'unique_key' : 'uniquely identifies this record',
                'annotation_key' : 'identifies the annotation',
                'reference_key' : 'identifies a reference for the annotation',
                'jnum_id' : 'J: number for the reference, cached here for convenience',
                'sequence_num' : 'used to order references for an annotation',
                },
        Table.INDEX : {
                'annotation_key' : 'clusters the data so all the references for an annotation may be quickly retrieved',
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
