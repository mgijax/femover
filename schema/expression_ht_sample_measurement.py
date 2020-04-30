#!./python

import Table

# contains data definition information for the expression_ht_sample_measurement table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_ht_sample_measurement'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        measurement_key         integer                         not null,
        sample_key                      integer                         not null,
        marker_key                      integer                         not null,
        average_tpm                     double precision        null,
        qn_tpm                          double precision        null,
        PRIMARY KEY(measurement_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'sample_key' : 'create index %s on %s (sample_key)',
        'marker_key' : 'create index %s on %s (marker_key)',
        }

# column name -> (related table, column in related table)
keys = {
        'sample_key' : ('expression_ht_sample', 'sample_key'),
        'marker_key' : ('marker', 'marker_key'),
        }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'Contains data for RNA-Seq experiments.  Has one record for each sample/marker pair.  If the sample is from a set of biological replicates, each sample from the set will have the same combined_rnaseq_key.',
        Table.COLUMN : {
                'measurement_key' : 'unique identifier for this row',
                'sample_key' : 'identifies the biological sample (genotype, age, sex, etc.)',
                'marker_key' : 'identifies the marker for this measurement',
                'average_tpm' : 'Transcripts Per (kilobase) Million, a normalized measure of the expression value for a gene in a sample, expressed as transcript-derived read counts from that gene per million reads in the sample.',
                'qn_tpm' : 'Quantile Normalized Transcripts Per (kilobase) Million, an industry standard for comparing or combining biological replicates in high throughput expression studies',
                },
        Table.INDEX : {
                'sample_key' : 'quick access to measurements for measurements for all genes for a particular sample',
                'marker_key' : 'quick lookup for all measurements for a marker by key',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
