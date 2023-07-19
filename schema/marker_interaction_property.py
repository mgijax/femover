#!./python

import Table

# contains data definition information for the marker_interaction_property table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_interaction_property'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int     not null,
        mi_key                  int     not null,
        name                    text    not null,
        value                   text    not null,
        sequence_num            int     not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {
        'mi_key' : ('marker_interaction', 'mi_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('miSeqNum',
        'create index %s on %s (mi_key, sequence_num)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'A record in this table represents a (name, value) property for a marker interacts with marker relationship',
        Table.COLUMN : {
                'unique_key' : 'primary key, uniquely identifying a record in this table',
                'mi_key' : 'foreign key, identifying the marker interacts with marker relationship for this property',
                'name' : 'name of the property',
                'value' : 'value of the property, as a string',
                'sequence_num' : 'integer, used to order properties for a given marker interacts with marker relationship',
                },
        Table.INDEX : {
                'miSeqNum' : 'clustered index ensures properties for a given relationship are stored in order together on disk',
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
