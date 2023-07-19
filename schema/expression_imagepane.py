#!./python

import Table

# contains data definition information for the expression_imagepane table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_imagepane'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        imagepane_key   int             not null,
        image_key       int             not null,
        pane_label      text    null,
        x       int             not null,
        y       int             not null,
        width   int             not null,
        height  int             not null,
        by_assay_type   int             not null,
        by_marker       int             not null,
        by_hybridization_asc    int     not null,
        by_hybridization_desc   int     not null,
        sequence_num    int             not null,
        PRIMARY KEY(imagepane_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'image_key' : ('image', 'image_key') }

clusteredIndex = ('image_key', 'create index %s on %s (image_key)')

comments = { Table.TABLE : 'petal table for the image flower, defining image panes (and their labels) for association with expression data',
        Table.COLUMN : {
                'imagepane_key' : 'unique identifier for this image pane',
                'image_key' : 'identifies the image containing this pane',
                'pane_label' : 'label used in the publication to identify this pane',
                'by_assay_type' : 'default sort for image pane in summaries',
                'by_marker' : 'gene column sort for image pane summary',
                'by_hybridization_asc' : 'default sort for hybridization/specimen type column in image summary',
                'by_hybridization_desc' : 'custom reverse sort for hybridization/specimen type column in image summary',
                'sequence_num' : 'used to order panes for each image',
                },
        Table.INDEX : {
                'image_key' : 'clusters data so that all panes for an image are stored together on disk for fast retrieval',
                }
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments,
        clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
