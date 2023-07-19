#!./python

import Table

# contains data definition information for the image table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'image'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        image_key               int             not null,
        reference_key           int             not null,
        thumbnail_image_key     int             null,
        fullsize_image_key      int             null,
        is_thumbnail            int             not null,
        width                   int             null,
        height                  int             null,
        figure_label            text    not null,
        image_type              text    null,
        pixeldb_numeric_id      text    null,
        mgi_id                  text    null,
        jnum_id                 text    null,
        copyright               text    null,
        caption                 text    null,
        external_link           text    null,
        image_class             text    null,
        PRIMARY KEY(image_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'reference_key' : 'create index %s on %s (reference_key)',
        }

keys = {
        'reference_key' : ('reference', 'reference_key'),
        'thumbnail_image_key' : ('image', 'image_key'),
        'fullsize_image_key' : ('image', 'image_key'),
        }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'central table for the image flower, containing basic data about images',
        Table.COLUMN : {
                'image_key' : 'unique key for an image; same as _Image_key in mgd',
                'reference_key' : 'identifies the reference that is the source of the image',
                'thumbnail_image_key' : 'key for the thumbnail version of this image (if this is a full-size image)',
                'fullsize_image_key' : 'key for the full-size version of this image (if this is a thumbnail image)',
                'is_thumbnail' : '1 if this is a thumbnail image, 0 if not',
                'width' : 'width of this image (in pixels)',
                'height' : 'height of this image (in pixels)',
                'figure_label' : 'figure label, as described in the reference',
                'image_type' : 'type of image (Full Size, Thumbnail)',
                'pixeldb_numeric_id' : 'numeric portion of the ID for this image in Pixel DB',
                'mgi_id' : 'MGI ID for this image',
                'jnum_id' : 'J: number ID for the reference, cached for convenience',
                'copyright' : 'copyright statement',
                'caption' : 'caption to display with the image',
                'external_link' : 'external link to display with the image',
                'image_class' : 'class of the image (Expression, Phenotypes, Molecular)',
                },
        Table.INDEX : {
                'reference_key' : 'look up images from a given reference',
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
