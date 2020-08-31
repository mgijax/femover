#!./python

import Table

# contains data definition information for the probe_alias table

###--- Globals ---###

# name of this database table
tableName = 'probe_alias'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key                              int             NOT NULL,
        probe_reference_key             int             NOT NULL,
        alias                                   text    NOT NULL,
        sequence_num                    int             NOT NULL,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = {
        'probe_reference_key' : ('probe_to_reference', 'probe_reference_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('probe_reference_key', 'create index %s on %s (probe_reference_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'stores aliases for probes as defined in a reference',
        Table.COLUMN : {
                'unique_key' : 'unique identifier to use as primary key (no other significance)',
                'probe_reference_key' : 'identifies the probe/reference pair; same as prb_reference._Reference_key in prod',
                'alias' : 'alias for the probe',
                'sequence_num' : 'pre-computed sequence number for smart-alpha ordering of aliases',
                },
        Table.INDEX : {
                'probe_reference_key' : 'clusters data so that all aliases for a probe/reference pair will be near each other on disk, aiding quick access',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
