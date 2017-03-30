#!/usr/local/bin/python
# 
# gathers data for the 'mapping_link' table in the front-end database

import Gatherer

###--- Globals ---###

JAXRH_URL = 'http://www.informatics.jax.org/downloads/datasets/rhmap/RHpanelMGIReport.txt'

###--- Classes ---###

MappingLinkGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the mapping_link table
	# Has: queries to execute against the source database
	# Does: queries the source database for extra links for mapping experiments,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	'''select e._Expt_key, 'RH Panel' as linkType, '%s' as url
		from bib_citation_cache c, mld_expts e
		where c.jnumID = 'J:68900'
		and e.exptType != 'CONTIG'
		and c._Refs_key = e._Refs_key''' % JAXRH_URL,
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Expt_key', 'linkType', 'url', ]

# prefix for the filename of the output file
filenamePrefix = 'mapping_link'

# global instance of a MappingLinkGatherer
gatherer = MappingLinkGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
