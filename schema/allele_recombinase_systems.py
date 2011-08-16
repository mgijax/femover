#!/usr/local/bin/python

import Table

# contains data definition information for the allele_recombinase_systems
# table

###--- Globals ---###

# name of this database table
tableName = 'allele_recombinase_systems'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	allele_key			int		not null,
	in_adipose_tissue		int		null,
	in_alimentary_system		int		null,
	in_branchial_arches		int		null,
	in_cardiovascular_system	int		null,
	in_cavities_and_linings		int		null,
	in_endocrine_system		int		null,
	in_head				int		null,
	in_hemolymphoid_system		int		null,
	in_integumental_system		int		null,
	in_limbs			int		null,
	in_liver_and_biliary_system	int		null,
	in_mesenchyme			int		null,
	in_muscle			int		null,
	in_nervous_system		int		null,
	in_renal_and_urinary_system	int		null,
	in_reproductive_system		int		null,
	in_respiratory_system		int		null,
	in_sensory_organs		int		null,
	in_skeletal_system		int		null,
	in_tail				int		null,
	in_early_embryo			int		null,
	in_extraembryonic_component	int		null,
	in_embryo_other			int		null,
	in_postnatal_other		int		null,
	detected_count			int		null,
	not_detected_count		int		null,
	PRIMARY KEY(allele_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'allele_key' : ('allele', 'allele_key') }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
