#!./python

import Table

# contains data definition information for the probe_to_reference table

###--- Globals ---###

# name of this database table
tableName = 'probe_reference_sequence'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key                              int             NOT NULL,
        probe_reference_key             int             NOT NULL,
        sequence_key                    int             NOT NULL,
        sequence_id                             text    NULL,
        qualifier                               text    NULL,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'sequence_key' : 'create index %s on %s (sequence_key)',
        }

keys = {
        'probe_reference_key' : ('probe_to_reference', 'probe_reference_key'),
        'sequence_key' : ('sequence', 'sequence_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('probe_reference_key', 'create index %s on %s (probe_reference_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'join table between probes/reference pairs and sequences',
        Table.COLUMN : {
                'unique_key' : 'generated primary key for this row, no other significance',
                'probe_reference_key' : 'identifies the probe/reference pair; same as prb_reference._Reference_key in prod',
                'sequence_key' : 'identifies the sequence',
                'sequence_id' : 'primary ID of the sequence, cached here for convenience',
                'qualifier' : 'qualifier describing this association',
                },
        Table.INDEX : {
                'probe_reference_key' : 'clusters data so that all sequences for a probe/reference pair will be near each other on disk, aiding quick access',
                'sequence_key' : 'look up all probe/reference pairs for a given sequence',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
