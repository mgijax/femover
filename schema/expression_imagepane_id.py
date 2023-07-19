#!./python

import Table

# contains data definition information for the expression_imagepane table

###--- Globals ---###

# name of this database table
tableName = 'expression_imagepane_id'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int     NOT NULL,
        imagepane_key   int     NOT NULL,
        logical_db      text NULL,
        acc_id          text NULL,
        preferred       int     NOT NULL,
        private         int     NOT NULL,
        sequence_num    int     NOT NULL,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'acc_id' : 'create index %s on %s (acc_id)',
        }

keys = { 'imagepane_key' : ('expression_imagepane', 'imagepane_key') }

# index used to cluster data in the table
clusteredIndex = ('imagepane_key', 'create index %s on %s (imagepane_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the image flower, providing accession IDs assigned to image panes related to expression assays',
        Table.COLUMN : {
                'unique_key' : 'uniquely identifies this record, no other purpose',
                'imagepane_key' : 'identifies the image pane',
                'logical_db' : 'logical database (provider) that assigned the ID',
                'acc_id' : 'unique ID for the image pane',
                'preferred' : '1 if this is the preferred ID for this pane for this logical db, 0 if not',
                'private' : '1 if this ID should be consiered to be private, 0 if it can be displayed',
                'sequence_num' : 'used for ordering IDs for an image pane',
                },
        Table.INDEX : {
                'imagepane_key' : 'clusters data so that all IDs for an image pane are stored together on disk, to aid quick access',
                'acc_id' : 'quick lookup of image panes by ID',
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
