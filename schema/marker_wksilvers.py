#!./python

import Table

# contains data definition information for the marker_wksilvers table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_wksilvers'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key                              int             not null,
        silvers_symbol                  text    not null,
        silvers_url_fragment    text    null,
        marker_key                              int             not null,
        marker_symbol                   text    not null,
        marker_id                               text    not null,
        sequence_num                    int             not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'marker_key' : 'create index %s on %s (marker_key)',
        'marker_id' : 'create index %s on %s (marker_id)',
        }

# column name -> (related table, column in related table)
keys = {
        'marker_key' : ('marker', 'marker_key')
        }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'contains data for generating the Rosetta table, mapping marker symbols used in the W.K.Silvers book to current MGI markers',
        Table.COLUMN : {
                'unique_key' : 'primary key for this table',
                'silvers_symbol' : 'symbol used by W.K. Silvers for this marker',
                'silvers_url_fragment' : 'partial URL for where the symbol appears in the book',
                'marker_key' : 'MGI marker key',
                'marker_symbol' : 'current official nomenclature for the marker',
                'marker_id' : 'current primary ID for the marker',
                'sequence_num' : 'pre-computed integer for sorting by silvers_symbol column',
                },
        Table.INDEX : {
                'marker_key' : 'easy lookup by MGI marker key',
                'marker_id' : 'easy lookup by primary marker ID',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
