#!./python

import Table

# contains data definition information for the
# recombinase_assay_result_imagepane table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'recombinase_assay_result_imagepane'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int             not null,
        result_key              int             not null,
        image_key               int             not null,
        sequence_num            int             not null,
        pane_label              text    null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = {
        'result_key' : ('recombinase_assay_result', 'result_key'),
        'image_key' : ('image', 'image_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('result_key', 'create index %s on %s (result_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'join table between recombinase assay results and their associated images',
        Table.COLUMN : {
                'unique_key' : 'unique identifier for this record',
                'result_key' : 'identifies the recombinase assay result',
                'image_key' : 'identifes an associated image',
                'sequence_num' : 'used to order images for a recombinase assay result',
                'pane_label' : 'label of the particular pane (within the image) that supports the assay result',
                },
        Table.INDEX : {
                'result_key' : 'clusters the data so all images for an assay result are stored together on disk, for effiency',
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
