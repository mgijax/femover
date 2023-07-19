#!./python

import Table

# contains data definition information for the gelband table 
# This object is essentially the gxd_gelband and gxd_gelrow tables smushed together

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'gelband'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        gelband_key             int             not null,
        gellane_key             int             not null,
        gelrow_key              int             not null,
        assay_key               int             not null,
        rowsize                 text    null,
        row_note                text    null,
        strength                text    null,
        band_note               text    null,
        row_seq         int             not null,
        PRIMARY KEY(gelband_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'assay_key' : 'create index %s on %s (assay_key)',
        'gellane_key' : 'create index %s on %s (gellane_key)',
        }

keys = {
        'assay_key' : ('expression_assay', 'assay_key'),
        'gellane_key' : ('gellane', 'gellane_key'),
        }

clusteredIndex=None
# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'Contains information about specimens for gelband objects',
        Table.COLUMN : {
                'gelband_key' : 'unique identifier for this band, same as _GelBand_key in mgd',
                'gellane_key' : 'unique identifier for this lane, same as _GelLane_key in mgd',
                'gelrow_key' : 'unique identifier for this row, same as _GelRow_key in mgd',
                'assay_key' : 'unique identifier for this assay, same as _Assay_key in mgd',
                'rowsize' : 'display text for row size in the Bands column',
                'row_note' : 'note for row',
                'strength' : 'strength value for the band',
                'band_note' : 'note for the gel band',
                'row_seq' : 'sequence order for the row (also used for the bands)',
                },
        Table.INDEX : {
                'gellane_key' : 'foreign key to gellane',
                'assay_key' : 'foreign key to assay',
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
