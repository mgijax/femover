#!/usr/local/bin/python
# 
# gathers data for the 'recombinase_expression' table in the front-end database

import Gatherer

###--- Classes ---###

RecombinaseExpressionGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the recombinase_expression table
	# Has: queries to execute against the source database
	# Does: queries the source database for expression data for recombinase alleles,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	'''select e._Emapa_Term_key as _Structure_key,
			r._Result_key,
			gag._Allele_key,
			rel._Object_key_2 as _Driver_key,
			case when st.strength = 'Absent' then 'No'
				when st.strength in ('Ambiguous', 'Not Specified', 'Not Applicable') then 'Ambiguous'
				else 'Yes'
				end as detected
		from gxd_expression e,
			gxd_specimen sp,
			gxd_insituresult r,
			gxd_strength st,
			gxd_allelegenotype gag,
			mgi_relationship rel,
			gxd_isresultstructure isr
		where e.isRecombinase = 1
			and e._Assay_key = sp._Assay_key
			and e._Specimen_key = sp._Specimen_key
			and sp._Specimen_key = r._Specimen_key
			and r._Strength_key = st._Strength_key
			and e._Genotype_key = gag._Genotype_key
			and r._Result_key = isr._Result_key
			and isr._Emapa_Term_key = e._Emapa_Term_key
			and rel._Category_key = 1006
			and rel._Object_key_1 = gag._Allele_key''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Driver_key', '_Allele_key', '_Structure_key', '_Result_key', 'detected'
	]

# prefix for the filename of the output file
filenamePrefix = 'recombinase_expression'

# global instance of a RecombinaseExpressionGatherer
gatherer = RecombinaseExpressionGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
