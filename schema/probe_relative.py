#!./python

import Table

# contains data definition information for the probe_relative table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'probe_relative'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key                      int             NOT NULL,
        probe_key                       int             NOT NULL,
        related_probe_key       int             NOT NULL,
        related_probe_id        text    NULL,
        related_probe_name      text    NULL,
        is_child                        int             NOT NULL,
        sequence_num            int             NOT NULL,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'related_probe_key' : 'create index %s on %s (related_probe_key)',
        'related_probe_id' : 'create index %s on %s (related_probe_id)',
        }

# column name -> (related table, column in related table)
keys = {
        'probe_key' : ('probe', 'probe_key'),
        'related_probe_key' : ('probe', 'probe_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('probe_key', 'create index %s on %s (probe_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'for a given probe, has probes it was derived from (is_child = 0) and probes derived from it (is_child = 1)',
        Table.COLUMN : {
                'unique_key' : 'primary key; uniquely identifies the record (no other significance)',
                'probe_key' : 'foreign key to probe table; identifies the probe we are gathering data for',
                'related_probe_key' : 'foreign key to probe table; identifies the related probe',
                'related_probe_id' : 'primary MGI ID of related probe, cached here for convenience',
                'related_probe_name' : 'name of related probe, cached here for convenience',
                'is_child' : 'flag to indicate if the related probe is derived from (1) the probe_key, or if the probe key is derived from (0) the related probe',
                'sequence_num' : 'pre-computed sequence number for ordering related probes',
                },
        Table.INDEX : {
                'related_probe_key' : 'quick reverse lookup by key',
                'related_probe_id' : 'quick reverse lookup by probe ID',
                'probe_key' : 'clustered index, for grouping all relatives of a probe together for fast retrieval',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
