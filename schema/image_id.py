#!./python

import Table

# contains data definition information for the image_id table

###--- Globals ---###

# name of this database table
tableName = 'image_id'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int     NOT NULL,
        image_key       int     NOT NULL,
        logical_db      text NULL,
        acc_id          text NULL,
        preferred       int     NOT NULL,
        private         int     NOT NULL,
        is_for_other_db_section int     NOT NULL,
        sequence_num    int     NOT NULL,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'acc_id' : 'create index %s on %s (acc_id)',
        }

keys = { 'image_key' : ('image', 'image_key') }

# index used to cluster data in the table
clusteredIndex = ('image_key', 'create index %s on %s (image_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the image flower, containing accession IDs for images',
        Table.COLUMN : {
                'unique_key' : 'uniquely identifies this record',
                'image_key' : 'identifies the image',
                'logical_db' : 'logical database (entity) assigning the accession ID',
                'acc_id' : 'ID for the image',
                'preferred' : '1 if this is the preferred ID for this image from this logical db, 0 if not',
                'private' : '1 if this ID should be considered private, 0 if it can be published',
                'is_for_other_db_section' : '1 if this ID should be displayed in the Other IDs section, 0 if not',
                'sequence_num' : 'for ordering IDs for each image',
                },
        Table.INDEX : {
                'image_key' : 'clusters data so all IDs for an image are together on disk, for quick access',
                'acc_id' : 'lookup an image by accession ID',
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
