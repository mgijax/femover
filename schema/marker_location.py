#!./python

import Table

# contains data definition information for the marker_location table

###--- Globals ---###

# name of this database table
tableName = 'marker_location'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int             NOT NULL,
        marker_key              int             NOT NULL,
        sequence_num            int             NOT NULL,
        chromosome              text    NULL,
        cm_offset               float           NULL,
        cytogenetic_offset      text    NULL,
        start_coordinate        float           NULL,
        end_coordinate          float           NULL,
        build_identifier        text    NULL,
        location_type           text    NOT NULL,
        map_units               text    NULL,
        provider                text    NULL,
        strand                  text    NULL,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'marker_key' : ('marker', 'marker_key'), }

# index used to cluster data in the table
clusteredIndex = ('marker_key',
        'create index %s on %s (marker_key, sequence_num)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table in the marker flower, containing locations for each marker',
        Table.COLUMN : {
                'unique_key' : 'unique key for this record, no other purpose',
                'marker_key' : 'identifies the marker',
                'sequence_num' : 'orders locations for a marker, with lower numbers indicating a more preferred location',
                'chromosome' : 'chromosome',
                'cm_offset' : 'centimorgan offset (cM)',
                'cytogenetic_offset' : 'cytogenetic offset (cytogenetic band)',
                'start_coordinate' : 'start genome coordinate',
                'end_coordinate' : 'end genome coordinate',
                'build_identifier' : 'names the genome build to which the coordinates are relative',
                'location_type' : 'which type of location is this:  cytogenetic, centimorgans, coordinates',
                'map_units' : 'measurement units for this location:  cM, bp, etc.',
                'provider' : 'name of the entity which assigned this location',
                'strand' : 'on which strand of DNA does this marker appear?  +, -, null',
                },
        Table.INDEX : {
                'marker_key' : 'clusters data so that all locations for a marker are stored near each other on disk, aiding quick retrieval',
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
