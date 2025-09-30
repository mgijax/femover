#!./python

# 
# gathers data for the 'marker_link' table in the front-end database

import Gatherer
import logger
import os
import MarkerUtils

###--- Constants ---###

# logical db
MGI = 1                
OMIM = 15
ENTREZ_GENE = 55
ZFIN_GENE = 172
RGD = 47
HGNC = 64
ENSEMBL_GENE_MODEL = 60

ldbLookup = {
        MGI:['MGI', 'accession_report.cgi?id='],
        OMIM:['OMIM', 'https://www.omim.org/entry/'],
        RGD:['Rat Genome Database', 'https://rgd.mcw.edu/rgdweb/report/gene/main.html?id='],
        ENTREZ_GENE:['Entrez Gene', 'https://www.ncbi.nlm.nih.gov/entrez/query.fcgi?db=gene&cmd=Retrieve&dopt=Graphics&list_uids='],
        ENSEMBL_GENE_MODEL:['Gene Tree', 'https:://useast.ensembl.org/Mus_musculus/Gene/Compara_Tree?db=core;g='],
        HGNC:['HGNC', 'https://www.genenames.org/data/gene-symbol-report/#!/hgnc_id/'],
        ZFIN_GENE:['Zebrafish Model Organism Database', 'https:://www.zfin.org/action/marker/view/']
}

# turned off until VISTA-Point comes up to new genome build (Build 39):
PROCESS_VISTA_POINT = 0
VISTA_POINT = 'VISTA-Point'
VISTA_POINT_URL = "https:://pipeline.lbl.gov/tbrowser/tbrowser/?pos=chr<chromosome>:<startCoordinate>-<endCoordinate>&base=<version>&run=21507#&base=2730&run=21507&genes=refseq&indx=0&cutoff=50"

# preferred ordering for links, by logical database (add Alliance links later)
LDB_ORDERING = [ HGNC, MGI, RGD, ZFIN_GENE, ENTREZ_GENE, OMIM, ENSEMBL_GENE_MODEL ]

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
HOMOLOGY_LINK_GROUP = 'homology gene links'    
ZFIN_EXPRESSION_LINK_GROUP = 'zfin expression links'
GEISHA_EXPRESSION_LINK_GROUP = 'geisha expression links'
XENBASE_EXPRESSION_LINK_GROUP = 'xenbase expression links'

# markup?
NO_MARKUPS = 0
HAS_MARKUPS = 1

# open in a new window?
NO_NEW_WINDOW = 0
USE_NEW_WINDOW = 1

ALLIANCE_MARKERS = set()

###-- links to external gene detail pages ---###

# URL for gene detail pages at Alliance (append HGNC, MGI, RGD, or ZFIN ID)
ALLIANCE_GENE_URL = 'https://www.alliancegenome.org/gene/'

###--- links to expression data ---###

# URL to expression data at zfin; substitute in an NCBI ID for a Zfin marker
zfin_exp_url = 'https://zfin.org/action/marker/%s/expression'

# URL to expression data at geisha; substitute in an NCBI ID for a chicken
# marker
geisha_exp_url = 'https:://geisha.arizona.edu/geisha/search.jsp?search=NCBI+ID&text=%s'

# URL to expression data at xenbase; substiture in an ID
xenbase_exp_url = 'https:://xenbase.org/gene/expression.do?method=displayGenePageExpression&entrezId=%s&tabId=1'

###--- Functions ---###

def markerLdbSortKey (a):
    # sort first by marker key, then by a custom sort for ldb key (preferred first); returns
    # tuple like (marker key, preferred ldb order, ldb name)

    aLdb = a[1]
    if aLdb in LDB_ORDERING:
        return (a[0], LDB_ORDERING.index(aLdb), aLdb)

    # non-preferred LDB, so push below the preferred ones
    return (a[0], 99, aLdb)

def sortResultSet (results, ldbKeyCol, markerKeyCol):
    # sort 'results' by marker key and a custom logical database sort

    # compile a sortable list that is indexed into 'results'
    i = 0
    sortable = []        # each item is (marker key, ldb key, i)

    for row in results:
        sortable.append ( (row[markerKeyCol], row[ldbKeyCol], i) )
        i = i + 1

    # sort the new list
    sortable.sort (key=markerLdbSortKey)

    # reshuffle results into a new list based on newly-sorted 'sortable'
    newList = []
    for (markerKey, ldbKey, i) in sortable:        newList.append (results[i])

    return newList

def excludeDuplicates (resultList, ldbKeyCol, markerKeyCol):
    # Purpose: remove from resultList any rows for which the same ldbKey
    #    and markerKey appear in multiple rows
    # Returns: updated version of 'resultList'
    # Assumes: nothing
    # Modifies: 'resultList'

    # We're going to do this in two passes.  First, we walk 'resultList'
    # and identify the (ldbKey, markerKey) pairs we need to remove.  Then
    # we will walk the list again and remove them.  This function, then,
    # will work regardless of the ordering of 'resultList'.

    seen = set()
    duplicated = set()

    i = len(resultList)
    while i > 0:
        i = i - 1
        pair = (resultList[i][ldbKeyCol], resultList[i][markerKeyCol])

        if pair in seen:
            duplicated.add(pair)
        else:
            seen.add(pair)

    # at this point, 'duplicated' contains the pairs we need to remove,
    # so we walk the list again and remove them

    i = len(resultList)
    while i > 0:
        i = i - 1
        pair = (resultList[i][ldbKeyCol], resultList[i][markerKeyCol])

        if pair in duplicated:
            del resultList[i]

    return resultList

def getDisplayText (ldbKey, ldbName, accID):
    # We can tweak the text of the link here, depending on the logical
    # database.  At present, we always use the logical db name.

    return ldbName

def getVersionForVistaPoint (version):
    # translate the coordinate version from our database to the version
    # identifier expected for the VISTA-Point link

    # currently no sophisticated logic; we just return mm39 (which is 
    # mouse build 39)

    return 'mm39'

###--- Classes ---###

class MarkerLinkGatherer (Gatherer.Gatherer):
    # Is: a data gatherer for the marker_link table
    # Has: queries to execute against the source database
    # Does: queries the source database for data we can use to pre-compute
    #    certain links, collates results, writes tab-delimited text
    #    file

    def getAllianceUrl (self, markerKey, ldbKey, accID):
        # Get the link for the detail page at the Alliance of Genome Resources (for the given data)
        
        url = ALLIANCE_GENE_URL
        if ldbKey == ZFIN_GENE:
            url = url + 'ZFIN:'
        return url + accID
    
    def getAllianceInfo (self, markerKey, ldbKey, accID):
        
        # If we need to generate a link to an Alliance detail page for the given data,
        # return it.  Otherwise return None.
        
        global ALLIANCE_MARKERS
        
        if markerKey not in ALLIANCE_MARKERS:
            if ldbKey in (ZFIN_GENE, MOUSE, HGNC, RGD):
                ALLIANCE_MARKERS.add(markerKey)
                return 'Alliance of Genome Resources', self.getAllianceUrl(markerKey, ldbKey, accID)
            
        return None

    def getHumanHomologyRows (self):
        # returns a list with one row for each link for each human
        # marker which is included in a homology cluster.  Each row
        # includes:
        #    [ 'marker_key', 'link_group', 'sequence_num',
        #    'associated_id', 'display_text', 'url', 'has_markups',
        #    'use_new_window' ]
        # Assumes: input rows are ordered by logical db key

        links = []

        cols, rows = self.results[0]

        ldbKeyCol = Gatherer.columnNumber (cols, '_LogicalDB_key')
        markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
        idCol = Gatherer.columnNumber (cols, 'accID')
        symbolCol = Gatherer.columnNumber (cols, 'symbol')

        seqNum = 0

        logger.debug ('Found %d human homology links' % len(rows))

        rows = sortResultSet (rows, ldbKeyCol, markerKeyCol)
        logger.debug ('Sorted %d human homology links' % len(rows))

        for row in excludeDuplicates (rows, ldbKeyCol, markerKeyCol):
            ldbKey = row[ldbKeyCol]
            markerKey = row[markerKeyCol]
            accID = row[idCol]
            seqNum = seqNum + 1

            if accID.startswith('OMIM:'):
                accID = accID.replace('OMIM:', '')
                
            ldbName = ldbLookup[ldbKey][0]
            fullUrl = ldbLookup[ldbKey][1] + accID

            links.append ( [ markerKey, HOMOLOGY_LINK_GROUP,
                seqNum, accID,
                getDisplayText(ldbKey, ldbName, accID), fullUrl, NO_MARKUPS, NO_NEW_WINDOW ] )

            allianceInfo = self.getAllianceInfo(markerKey, ldbKey, accID)
            if allianceInfo != None:
                name, url = allianceInfo
                links.append ( [ markerKey, HOMOLOGY_LINK_GROUP, seqNum + 10, row[symbolCol], name, url, NO_MARKUPS, NO_NEW_WINDOW ] )

        logger.debug ('Stored %d human homology links' % seqNum)
        return links

    def getMouseHomologyRows (self):
        # returns a list with one row for each link for each mouse
        # marker which is included in a homology cluster.  Each row
        # includes:
        #    [ 'marker_key', 'link_group', 'sequence_num',
        #    'associated_id', 'display_text', 'url', 'has_markups',
        #    'use_new_window' ]
        # Assumes: input rows are ordered by logical db key
        # Notes: We have links both by IDs and by coordinates.

        links = []

        # build the ID-based links first, as they need lower sequence
        # numbers for ordering

        cols, rows = self.results[1]

        ldbKeyCol = Gatherer.columnNumber (cols, '_LogicalDB_key')
        markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
        idCol = Gatherer.columnNumber (cols, 'accID')
        symbolCol = Gatherer.columnNumber (cols, 'symbol')

        seqNum = 0

        logger.debug ('Found %d ID-based mouse homology links' % len(rows))
        rows = sortResultSet (rows, ldbKeyCol, markerKeyCol)
        logger.debug ('Sorted %d ID-based mouse homology links' % len(rows))

        for row in excludeDuplicates (rows, ldbKeyCol, markerKeyCol):
            ldbKey = row[ldbKeyCol]
            markerKey = row[markerKeyCol]
            accID = row[idCol]
            seqNum = seqNum + 1

            ldbName = ldbLookup[ldbKey][0]
            fullUrl = ldbLookup[ldbKey][1] + accID

            if ldbKey == 60:
                displayID = None
            else:
                displayID = accID

            links.append ( [ markerKey, HOMOLOGY_LINK_GROUP,
                seqNum, displayID,
                getDisplayText(ldbKey, ldbName, accID), fullUrl, NO_MARKUPS, NO_NEW_WINDOW ] )
            
            allianceInfo = self.getAllianceInfo(markerKey, ldbKey, accID)
            if allianceInfo != None:
                name, url = allianceInfo
                links.append ( [ markerKey, HOMOLOGY_LINK_GROUP, seqNum + 10, row[symbolCol], name, url, NO_MARKUPS, NO_NEW_WINDOW ] )

        # build the coordinate-based links next, as they should come
        # last in each marker's list of links

        # if not processing VISTA_POINT, return
        if PROCESS_VISTA_POINT == 0:
                logger.debug ('Stored %d mouse homology links' % seqNum)
                return links

        # else, process VISTA_POINT
        cols, rows = self.results[2]

        logger.debug ('Found %d coord-based mouse homology links' % len(rows))

        markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
        chromCol = Gatherer.columnNumber (cols, 'genomicChromosome')
        startCol = Gatherer.columnNumber (cols, 'startCoordinate')
        endCol = Gatherer.columnNumber (cols, 'endCoordinate')
        strandCol = Gatherer.columnNumber (cols, 'strand')
        versionCol = Gatherer.columnNumber (cols, 'version')

        seen = set()    # track which marker keys we already handled

        for row in rows:
            markerKey = row[markerKeyCol]

            # don't make extra links if the marker happens to
            # return multiple coordinate rows -- just keep the
            # first one

            if markerKey in seen:
                continue
            seen.add(markerKey)

            chromosome = row[chromCol]
            startCoord = str(int(row[startCol]))
            endCoord = str(int(row[endCol]))
            version = getVersionForVistaPoint(row[versionCol])
            seqNum = seqNum + 1
            vistaPointUrl = VISTA_POINT_URL
            vistaPointUrl = vistaPointUrl.replace ( '<version>', version)
            vistaPointUrl = vistaPointUrl.replace ( '<chromosome>', chromosome)
            vistaPointUrl = vistaPointUrl.replace ( '<startCoordinate>', startCoord)
            vistaPointUrl = vistaPointUrl.replace ( '<endCoordinate>', endCoord)
            links.append ( [ markerKey, HOMOLOGY_LINK_GROUP, seqNum, None, VISTA_POINT, vistaPointUrl, NO_MARKUPS, NO_NEW_WINDOW ] ) 

        logger.debug ('Stored %d mouse homology links' % seqNum)
        return links

    def getOtherHomologyRows (self):
        # returns a list with one row for each link for each non-human,
        # non-mouse marker which is included in a homology cluster.
        # Each row includes:
        #    [ 'marker_key', 'link_group', 'sequence_num',
        #    'associated_id', 'display_text', 'url', 'has_markups',
        #    'use_new_window' ]
        # Assumes: input rows are ordered by logical db key

        links = []

        cols, rows = self.results[3]

        ldbKeyCol = Gatherer.columnNumber (cols, '_LogicalDB_key')
        markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
        idCol = Gatherer.columnNumber (cols, 'accID')
        symbolCol = Gatherer.columnNumber (cols, 'symbol')

        seqNum = 0

        logger.debug ('Found %d other homology links' % len(rows))

        rows = sortResultSet (rows, ldbKeyCol, markerKeyCol)
        logger.debug ('Sorted %d other homology links' % len(rows))

        for row in excludeDuplicates (rows, ldbKeyCol, markerKeyCol):
            ldbKey = row[ldbKeyCol]
            markerKey = row[markerKeyCol]
            accID = row[idCol]
            seqNum = seqNum + 1

            ldbName = ldbLookup[ldbKey][0]
            fullUrl = ldbLookup[ldbKey][1] + accID

            links.append ( [ markerKey, HOMOLOGY_LINK_GROUP,
                seqNum, accID,
                getDisplayText(ldbKey, ldbName, accID), fullUrl, NO_MARKUPS, NO_NEW_WINDOW ] )
            
            allianceInfo = self.getAllianceInfo(markerKey, ldbKey, accID)
            if allianceInfo != None:
                name, url = allianceInfo
                links.append ( [ markerKey, HOMOLOGY_LINK_GROUP, seqNum + 10, row[symbolCol], name, url, NO_MARKUPS, NO_NEW_WINDOW ] )

        logger.debug ('Stored %d other homology links' % seqNum)
        return links

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

        self.finalResults = self.getHumanHomologyRows() + \
                    self.getMouseHomologyRows() + \
                    self.getOtherHomologyRows() + \
                    self.getOrganismRows(4, 'mouse_marker_key', 'zfin_symbol', 'zfin_entrezgene_id', zfin_exp_url, ZFIN_EXPRESSION_LINK_GROUP) + \
                    self.getOrganismRows(5, 'mouse_marker_key', 'geisha_symbol', 'geisha_entrezgene_id', geisha_exp_url, GEISHA_EXPRESSION_LINK_GROUP) + \
                    self.getOrganismRows(6, 'mouse_marker_key', 'xenbase_symbol', 'xenbase_entrezgene_id', xenbase_exp_url, XENBASE_EXPRESSION_LINK_GROUP)

        logger.debug ('Found %d total links' % \
            len(self.finalResults))
        return

###--- globals ---###

# present rules for links for markers (in order of display):
# 1. links for human genes:
#    a. HGNC (HGNC ID)
#    b. EntrezGene (EntrezGene ID)
#    c. OMIM (OMIM gene ID)
# 2. links for mouse genes:
#    a. MGI marker detail (MGI ID)
#    b. EntrezGene (EntrezGene ID)
#    c. GeneTree (Ensembl ID -- if only one Ensembl ID)
#    d. VISTA-Point (marker coordinates)
# 3. links for other organisms' genes:
#    a. EntrezGene (EntrezGene ID)

cmds = [
    # 0. human markers' IDs
    '''select a._LogicalDB_key,
        m._Marker_key,
        a.accID,
        m.symbol
    from mrk_marker m, acc_accession a
    where m._Organism_key = %d
        and m._Marker_key = a._Object_key
        and a._MGIType_key = %d
        and a._LogicalDB_key in (%d, %d, %d)
        and a.private = 0
        and a.preferred = 1''' % (HUMAN, MARKER, HGNC, ENTREZ_GENE, OMIM),

    # 1. mouse markers' IDs
    '''select a._LogicalDB_key,
        m._Marker_key,
        a.accID,
        m.symbol
    from mrk_marker m, acc_accession a
    where m._Organism_key = %d
        and m._Marker_key = a._Object_key
        and a._MGIType_key = %d
        and a._LogicalDB_key in (%d, %d, %d)
        and a.private = 0
        and a.preferred = 1''' % (
            MOUSE, MARKER, MGI, ENTREZ_GENE, ENSEMBL_GENE_MODEL),

    # 2. mouse markers' coordinates
    '''select mlc._Marker_key,
        mlc.sequenceNum,
        mlc.genomicChromosome,
        mlc.startCoordinate,
        mlc.endCoordinate,
        mlc.strand,
        mlc.version
    from mrk_location_cache mlc
    where mlc._Organism_key = %d
        and mlc.genomicChromosome is not null
        and mlc.startCoordinate is not null
        and mlc.endCoordinate is not null''' % MOUSE, 

    # 3. other species' markers
    '''select a._LogicalDB_key,
        m._Marker_key,
        a.accID,
        m.symbol
    from mrk_marker m, acc_accession a
    where m._Organism_key != %d
        and m._Organism_key != %d
        and m._Marker_key = a._Object_key
        and a._MGIType_key = %d
        and a._LogicalDB_key in (%d, %d, %d)
        and (a.private = 0 or a._LogicalDB_key = %d)
        and a.preferred = 1''' % (
            HUMAN, MOUSE, MARKER, ENTREZ_GENE, ZFIN_GENE, RGD, RGD),

    # 4. data for ZFIN expression links (via homology)
    '''select distinct m._Marker_key as mouse_marker_key,
        zm.symbol as zfin_symbol,
        za.accID as zfin_entrezgene_id
    from mrk_marker m, mrk_clustermember mcm, mrk_cluster mc,
        mrk_clustermember zcm, mrk_marker zm, acc_accession za
    where m._Marker_Status_key = 1
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
            ZFIN_SOURCE, ZEBRAFISH, MARKER, ZFIN_GENE),

    # 5. data for chicken expression links (via homology)
    '''select distinct m._Marker_key as mouse_marker_key,
        zm.symbol as geisha_symbol,
        za.accID as geisha_entrezgene_id
    from mrk_marker m, mrk_clustermember mcm, mrk_cluster mc,
        mrk_clustermember zcm, mrk_marker zm, acc_accession za
    where m._Marker_Status_key = 1
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
    
    # 6. data for xenbase expression links (via homology)
    '''select distinct m._Marker_key as mouse_marker_key,
        zm.symbol as xenbase_symbol,
        za.accID as xenbase_entrezgene_id
    from mrk_marker m, mrk_clustermember mcm, mrk_cluster mc,
        mrk_clustermember zcm, mrk_marker zm, acc_accession za
    where m._Marker_Status_key = 1
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
