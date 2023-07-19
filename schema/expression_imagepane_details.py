#!./python

import Table

# contains data definition information for the expression_imagepane_details
# table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_imagepane_details'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int             not null,
        imagepane_key   int             not null,
        assay_key       int             not null,
        assay_id        text    null,
        marker_key      int             not null,
        marker_id       text    null,
        marker_symbol   text    null,
        sequence_num    int             not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'assay_key' : 'create index %s on %s (assay_key)',
        'marker_key' : 'create index %s on %s (marker_key)',
        }

keys = {
        'imagepane_key' : ('expression_imagepane', 'imagepane_key'),
        'assay_key' : ('expression_assay', 'assay_key'),
        'marker_key' : ('marker', 'marker_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('imagepane_key', 'create index %s on %s (imagepane_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the image flower, providing details for objects related to expression_imagepane data (another petal table)',
        Table.COLUMN : {
                'unique_key' : 'unique identifier for this record, no other significance',
                'imagepane_key' : 'identifies the image pane',
                'assay_key' : 'identifies an expression assay associated with this image pane',
                'assay_id' : 'primary ID for the assay, cached for convenience',
                'marker_key' : 'identifies the marker studied in the assay',
                'marker_id' : 'primary ID for the marker, cached for convenience',
                'marker_symbol' : 'symbol for the marker, cached for convenience',
                'sequence_num' : 'used to order assay/marker pairs for an image pane for display',
                },
        Table.INDEX : {
                'imagepane_key' : 'clusters data to group all records for an image pane together for quick access',
                'assay_key' : 'look up image panes for an assay',
                'marker_key' : 'look up image panes for a marker',
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
