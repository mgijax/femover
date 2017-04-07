#!/usr/local/bin/python
# 
# gathers data for the 'mapping_fish' table in the front-end database

import Gatherer

###--- Classes ---###

MappingFishGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the mapping_fish table
	# Has: queries to execute against the source database
	# Does: queries the source database for supplemental data for FISH mapping experiments,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	'''select e._Expt_key, s.strain, e.band, e.cellorigin, e.karyotype, e.robertsonians,
			e.nummetaphase, e.totalsingle, e.totaldouble, e.label
		from MLD_FISH e, PRB_Strain s
		where e._Strain_key = s._Strain_key''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Expt_key', 'strain', 'band', 'cellorigin', 'karyotype', 'robertsonians',
	'nummetaphase', 'totalsingle', 'totaldouble', 'label',
	]

# prefix for the filename of the output file
filenamePrefix = 'mapping_fish'

# global instance of a MappingFishGatherer
gatherer = MappingFishGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
