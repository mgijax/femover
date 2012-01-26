#!/usr/local/bin/python
# 
# gathers data for the 'anatomy_structures_synonyms' table in the front-end
# database

import Gatherer

###--- Classes ---###

AnatomyGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the anatomy_structures_synonyms table
	# Has: queries to execute against the source mgd database
	# Does: queries for primary data for strucures and synonyms from the
	#	anatomical dictionary, collates results, writes tab-delimited
	#	text file


###--- globals ---###

cmds = [ '''select distinct str.structure, syn.structure as synonym
	from GXD_Structure s
	inner join GXD_StructureName str
		on (s._StructureName_key = str._StructureName_key
		    and str.structure != ' ')
	left outer join GXD_StructureName syn
		on (s._Structure_key = syn._Structure_key
		    and s._StructureName_key != syn._StructureName_key)''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, 'structure', 'synonym',
	]

# prefix for the filename of the output file
filenamePrefix = 'anatomy_structures_synonyms'

# global instance of a AnatomyGatherer
gatherer = AnatomyGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
