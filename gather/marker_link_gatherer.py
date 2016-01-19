#!/usr/local/bin/python
# 
# gathers data for the 'marker_link' table in the front-end database

import config
import Gatherer
import logger
import os
import MarkerUtils

###--- Constants ---###

# logical db
MGI = 1				
OMIM = 15
ENTREZ_GENE = 55


# organism 
MOUSE = 1			
HUMAN = 2
ZEBRAFISH = 84
CHICKEN = 63
XENBASE = 95

# MGI Type
MARKER = 2			

# marker cluster type (vocab term)
HOMOLOGY_CLUSTER = 9272150	

# marker cluster source (vocab term)
ZFIN_SOURCE = 13575996
GEISHA_SOURCE = 13575998
XENBASE_SOURCE = 13611349

# link group name
ZFIN_EXPRESSION_LINK_GROUP = 'zfin expression links'
GEISHA_EXPRESSION_LINK_GROUP = 'geisha expression links'
XENBASE_EXPRESSION_LINK_GROUP = 'xenbase expression links'

# markup?
NO_MARKUPS = 0
HAS_MARKUPS = 1

# open in a new window?
NO_NEW_WINDOW = 0
USE_NEW_WINDOW = 1

# URL to expression data at zfin; substitute in an NCBI ID for a Zfin marker
zfin_url = '''http://zfin.org/cgi-bin/webdriver?MIval=aa-xpatselect.apg&query_results=exist&START=0&searchtype=equals&gene_name=%s'''

# URL to expression data at geisha; substitute in an NCBI ID for a chicken
# marker
geisha_url = 'http://geisha.arizona.edu/geisha/search.jsp?search=NCBI+ID&text=%s'

# URL to express data at xenbase; substiture in an ID
xenbase_url = 'http://xenbase.org/gene/expression.do?method=displayGenePageExpression&entrezId=%s&tabId=1'

###--- Functions ---###


###--- Classes ---###

class MarkerLinkGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the marker_link table
	# Has: queries to execute against the source database
	# Does: queries the source database for data we can use to pre-compute
	#	certain links, collates results, writes tab-delimited text
	#	file


	def getOrganismRows(self, resultIndex, markerKeyColumn, organismSymbolColumn, organismIdColumn, organismUrl, linkGroup):
		cols, rows = self.results[resultIndex]

		markerKeyCol = Gatherer.columnNumber (cols, markerKeyColumn)
		symbolCol = Gatherer.columnNumber (cols, organismSymbolColumn)
		idCol = Gatherer.columnNumber (cols, organismIdColumn)
		
		seqNum = 0
		hasMarkups = 0
		useNewWindow = 1

		out = []

		for row in rows:
			markerKey = row[markerKeyCol]
			organismSymbol = row[symbolCol]
			organismID = row[idCol]

			seqNum = seqNum + 1

			out.append( [ markerKey, linkGroup, seqNum, organismID, organismSymbol, organismUrl % organismID, hasMarkups, useNewWindow ] )

		logger.debug('Returning %d rows for expression links' % len(out))
		return out

	def collateResults (self):
		self.finalColumns = [ 'marker_key', 'link_group',
			'sequence_num', 'associated_id', 'display_text',
			'url', 'has_markups', 'use_new_window' ]

		self.finalResults = self.getOrganismRows(0, 'mouse_marker_key', 'zfin_symbol', 'zfin_entrezgene_id', zfin_url, ZFIN_EXPRESSION_LINK_GROUP) + \
				    self.getOrganismRows(1, 'mouse_marker_key', 'geisha_symbol', 'geisha_entrezgene_id', geisha_url, GEISHA_EXPRESSION_LINK_GROUP) + \
				    self.getOrganismRows(2, 'mouse_marker_key', 'xenbase_symbol', 'xenbase_entrezgene_id', xenbase_url, XENBASE_EXPRESSION_LINK_GROUP)

		logger.debug ('Found %d total links' % \
			len(self.finalResults))
		return

###--- globals ---###

cmds = [
	# 0. data for ZFIN expression links (via homology)
	'''select distinct m._Marker_key as mouse_marker_key,
		zm.symbol as zfin_symbol,
		za.accID as zfin_entrezgene_id
	from mrk_marker m, mrk_clustermember mcm, mrk_cluster mc,
		mrk_clustermember zcm, mrk_marker zm, acc_accession za
	where m._Marker_Status_key in (1,3)
		and m._Organism_key = %d
		and m._Marker_key = mcm._Marker_key
		and mcm._Cluster_key = mc._Cluster_key
		and mc._ClusterType_key = %d
		and mc._ClusterSource_key = %d
		and mc._Cluster_key = zcm._Cluster_key
		and zcm._Marker_key = zm._Marker_key
		and zm._Organism_key = %d
		and zcm._Marker_key = za._Object_key
		and za._MGIType_key = %d
		and za._LogicalDB_key = %d
		and za.private = 0
		and za.preferred = 1
	order by m._Marker_key, zm.symbol''' % (MOUSE, HOMOLOGY_CLUSTER,
			ZFIN_SOURCE, ZEBRAFISH, MARKER, ENTREZ_GENE),

	# 1. data for chicken expression links (via homology)
	'''select distinct m._Marker_key as mouse_marker_key,
		zm.symbol as geisha_symbol,
		za.accID as geisha_entrezgene_id
	from mrk_marker m, mrk_clustermember mcm, mrk_cluster mc,
		mrk_clustermember zcm, mrk_marker zm, acc_accession za
	where m._Marker_Status_key in (1,3)
		and m._Organism_key = %d
		and m._Marker_key = mcm._Marker_key
		and mcm._Cluster_key = mc._Cluster_key
		and mc._ClusterType_key = %d
		and mc._ClusterSource_key = %d
		and mc._Cluster_key = zcm._Cluster_key
		and zcm._Marker_key = zm._Marker_key
		and zm._Organism_key = %d
		and zcm._Marker_key = za._Object_key
		and za._MGIType_key = %d
		and za._LogicalDB_key = %d
		and za.private = 0
		and za.preferred = 1
	order by m._Marker_key, zm.symbol''' % (MOUSE, HOMOLOGY_CLUSTER,
			GEISHA_SOURCE, CHICKEN, MARKER, ENTREZ_GENE),
	
	# 2. data for xenbase expression links (via homology)
	'''select distinct m._Marker_key as mouse_marker_key,
		zm.symbol as xenbase_symbol,
		za.accID as xenbase_entrezgene_id
	from mrk_marker m, mrk_clustermember mcm, mrk_cluster mc,
		mrk_clustermember zcm, mrk_marker zm, acc_accession za
	where m._Marker_Status_key in (1,3)
		and m._Organism_key = %d
		and m._Marker_key = mcm._Marker_key
		and mcm._Cluster_key = mc._Cluster_key
		and mc._ClusterType_key = %d
		and mc._ClusterSource_key = %d
		and mc._Cluster_key = zcm._Cluster_key
		and zcm._Marker_key = zm._Marker_key
		and zm._Organism_key = %d
		and zcm._Marker_key = za._Object_key
		and za._MGIType_key = %d
		and za._LogicalDB_key = %d
		and za.private = 0
		and za.preferred = 1
	order by m._Marker_key, zm.symbol''' % (MOUSE, HOMOLOGY_CLUSTER,
			XENBASE_SOURCE, XENBASE, MARKER, ENTREZ_GENE),
	]


# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, 'marker_key', 'link_group', 'sequence_num',
	'associated_id', 'display_text', 'url', 'has_markups',
	'use_new_window'
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_link'

# global instance of a MarkerLinkGatherer
gatherer = MarkerLinkGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
