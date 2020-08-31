#!./python

import Table

# contains data definition information for the hdp_genocluster_marker table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
# human disease portal
tableName = 'hdp_genocluster_marker'

#
# statement to create this table
#
# This table represents the relationship between markers and genoclusters
#
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int     not null,
        hdp_genocluster_key             int             not null,
        marker_key                              int             not null,
        symbol                                  text    null,
        primary_id                              text    null,
        marker_type                             text    null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'marker_key' : 'create index %s on %s (marker_key)',
        'primary_id' : 'create index %s on %s (primary_id)',
        }

keys = {
        'marker_key' : ('marker', 'marker_key'),
        'hdp_genocluster_key' : ('hdp_genocluster', 'hdp_genocluster_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('hdp_genocluster_key', 'create index %s on %s (hdp_genocluster_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'join table between genoclusters and markers, allowing for multiple markers per genocluster (common for transgenes)',
        Table.COLUMN : {
                'unique_key' : 'unique key for this record',
                'hdp_genocluster_key' : 'foreign key to hdp_genocluster table',
                'marker_key' : 'foreign key to marker table',
                'symbol' : 'marker symbol',
                'primary_id' : 'primary accession ID of marker',
                'marker_type' : 'type of marker',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
