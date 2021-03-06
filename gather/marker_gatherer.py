#!./python
# 
# gathers data for the 'marker' table in the front-end database
#
# lec   08/06/2013
#       - TR11423/human disease portal/scrum-dog
#               added location/coordinate display
#

import Gatherer
import logger
import GOGraphs
import utils
import dbAgnostic

# list of markers in mrk_location_cache
locationLookup = {}

coordinateDisplay1 = 'Chr%s:%s-%s (%s)'
coordinateDisplay2 = 'Chr%s:%s-%s'
locationDisplay1 = 'Chr%s %s cM'
locationDisplay2 = 'Chr%s %s'
locationDisplay3 = 'Chr%s syntenic'
locationDisplay4 = 'Chr%s QTL'
locationDisplay5 = 'Chr Unknown'

strainName = 'C57BL/6J'
strainID = None

###--- Functions ---###

def getStrainID():
        # get the primary ID for the global 'strainName'
        global strainID
        
        if strainID == None:
                cmd = '''select a.accID
                        from prb_strain ps, acc_accession a, acc_mgitype t
                        where ps._Strain_key = a._Object_key
                                and a._MGIType_key = t._MGIType_key
                                and t.name = 'Strain'
                                and a._LogicalDB_key = 1
                                and a.preferred = 1
                                and ps.strain = '%s' ''' % strainName
                cols, rows = dbAgnostic.execute(cmd)
                
                if len(rows) != 1:
                        raise Exception('Cannot uniquely find strain %s' % strainName)
                
                strainID = rows[0][0]
                logger.debug('Found ID for %s' % strainName)

        return strainID

def getLocationDisplay (marker, organism):
        #
        # returns location display
        # returns coordinate display
        # returns build identifier (version)
        #

        if marker not in locationLookup:
                return '', '', ''

        gchromosome = locationLookup[marker][1]
        chromosome = locationLookup[marker][2]
        startCoordinate = locationLookup[marker][3]
        endCoordinate = locationLookup[marker][4]
        strand = locationLookup[marker][5]
        cmoffset = locationLookup[marker][6]
        cytooffset = locationLookup[marker][7]
        buildIdentifier = locationLookup[marker][8]
        markerType = locationLookup[marker][9]

        if organism == 1:
                if chromosome == 'UN' or cmoffset == -999:
                        location = locationDisplay5
                elif markerType == 6:
                        location = locationDisplay4 % (chromosome)
                elif cmoffset == -1:
                        location = locationDisplay3 % (chromosome)
                else:
                        location = locationDisplay1 % (chromosome, cmoffset)

        elif cytooffset:
                location = locationDisplay2 % (chromosome, cytooffset)

        else:
                location = ''

        if startCoordinate:
                if strand:
                        coordinate = coordinateDisplay1 \
                                % (gchromosome, startCoordinate, endCoordinate, strand)
                else:
                        coordinate = coordinateDisplay2 \
                                % (gchromosome, startCoordinate, endCoordinate)
        else:
                coordinate = ''

        return location, coordinate, buildIdentifier

###--- Classes ---###

class MarkerGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the marker table
        # Has: queries to execute against source db
        # Does: queries for primary data for markers, collates results,
        #       writes tab-delimited text file

        def collateResults (self):
                global locationLookup

                self.featureTypes = {}  # marker key -> [ feature types ]
                self.ids = {}           # marker key -> (id, logical db key)

                # feature types from MCV vocab are in query 0 results

                cols, rows = self.results[0]
                keyCol = Gatherer.columnNumber (cols, '_Marker_key')
                termCol = Gatherer.columnNumber (cols, 'directTerms')

                for row in rows:
                        key = row[keyCol]
                        term = row[termCol]

                        if key in self.featureTypes:
                                self.featureTypes[key].append (term)
                        else:
                                self.featureTypes[key] = [ term ]

                logger.debug ('Found %d MCV terms for %d markers' % (
                        len(rows), len(self.featureTypes) ) )

                # IDs are retrieved in query 1

                cols, rows = self.results[1]
                keyCol = Gatherer.columnNumber (cols, '_Marker_key')
                idCol = Gatherer.columnNumber (cols, 'accid')
                ldbCol = Gatherer.columnNumber (cols, '_LogicalDB_key')

                for row in rows:
                        self.ids[row[keyCol]] = (row[idCol], row[ldbCol])
        
                logger.debug ('Found %d primary IDs for markers' % \
                        len(self.ids))

                # sql (2) -- location data for each marker
                (cols, rows) = self.results[2]

                # set of columns for common sql fields
                keyCol = Gatherer.columnNumber (cols, '_Marker_key')

                for row in rows:
                        locationLookup[row[keyCol]] = row

                # last query has the bulk of the data
                self.finalColumns = self.results[-1][0]
                self.finalResults = self.results[-1][1]

                logger.debug ('Found %d markers' % len(self.finalResults))
                return

        def postprocessResults (self):
                # Purpose: override method to provide key-based lookups

                self.convertFinalResultsToList()

                statusCol = Gatherer.columnNumber (self.finalColumns, '_Marker_Status_key')
                typeCol = Gatherer.columnNumber (self.finalColumns, '_Marker_Type_key')
                orgCol = Gatherer.columnNumber (self.finalColumns, '_Organism_key')
                keyCol = Gatherer.columnNumber (self.finalColumns, '_Marker_key')

                B6Name = strainName
                B6ID = getStrainID()
                
                for r in self.finalResults:

                        markerKey = r[keyCol]
                        organism = r[orgCol]

                        if markerKey in self.featureTypes:
                                feature = ', '.join (self.featureTypes[markerKey])
                        else:
                                feature = None

                        # look up cached ID and logical database
                        if markerKey in self.ids:
                                accid, ldb = self.ids[markerKey]
                                ldbName = Gatherer.resolve (ldb, 'acc_logicaldb', '_LogicalDB_key', 'name')
                        else:
                                accid = None
                                ldb = None
                                ldbName = None

                        organism = utils.cleanupOrganism(Gatherer.resolve (r[orgCol], 'mgi_organism', '_Organism_key', 'commonName'))

                        self.addColumn ('accid', accid, r, self.finalColumns)
                        self.addColumn ('subtype', feature, r, self.finalColumns)
                        self.addColumn ('status', Gatherer.resolve (r[statusCol], 'mrk_status', '_Marker_Status_key', 'status'), r, self.finalColumns)
                        self.addColumn ('logicalDB', ldbName, r, self.finalColumns)
                        self.addColumn ('markerType', Gatherer.resolve (r[typeCol], 'mrk_types', '_Marker_Type_key', 'name'), r, self.finalColumns)
                        self.addColumn ('organism', organism, r, self.finalColumns)
                        self.addColumn ('hasGOGraph', GOGraphs.hasGOGraph(accid), r, self.finalColumns)

                        # location and coordinate information
                        location, coordinate, buildIdentifier = getLocationDisplay(markerKey, organism)
                        self.addColumn ('location_display', location, r, self.finalColumns)
                        self.addColumn ('coordinate_display', coordinate, r, self.finalColumns)
                        self.addColumn ('build_identifier', buildIdentifier, r, self.finalColumns)
                        
                        # add strain info for markers with coordinates on C57BL/6J
                        if (organism == 'mouse') and coordinate:
                                self.addColumn ('strain', B6Name, r, self.finalColumns)
                                self.addColumn ('strain_id', B6ID, r, self.finalColumns)
                        else:
                                self.addColumn ('strain', None, r, self.finalColumns)
                                self.addColumn ('strain_id', None, r, self.finalColumns) 
                return

###--- globals ---###

cmds = [
        # 0. get the MCV (feature type) annotations, from which we will build
        # the subtype for each marker
        'select distinct _Marker_key, directTerms from MRK_MCV_Cache',

        # 1. Gather the set of primary marker IDs
        # (mouse markers on the top of the union , non-mouse markers below)
        # human below mouse, use Entrez Gene IDs
        '''select m._Marker_key,
                a.accID, 
                a._LogicalDB_key
        from mrk_marker m,
                acc_accession a
        where m._Marker_key = a._Object_key
                and a._MGIType_key = 2
                and a._LogicalDB_key = 1
                and a.preferred = 1
                and m._Organism_key = 1
        union
        select m._Marker_key,
                a.accID, 
                a._LogicalDB_key
        from mrk_marker m,
                acc_accession a,
                acc_logicaldb ldb
        where m._Marker_key = a._Object_key
                and a._MGIType_key = 2
                and m._Organism_key = 2
                and a.preferred = 1
                and a._logicalDB_key = 55
                and a._LogicalDB_key = ldb._LogicalDB_key
        union
        select m._Marker_key,
                a.accID, 
                a._LogicalDB_key
        from mrk_marker m,
                acc_accession a,
                acc_logicaldb ldb
        where m._Marker_key = a._Object_key
                and a._MGIType_key = 2
                and m._Organism_key not in (1,2)
                and a.preferred = 1
                and a._LogicalDB_key = ldb._LogicalDB_key
                and ldb._Organism_key = m._Organism_key''',

        # 2. all markers with mrk_location_cache (mouse/human only)
        #    + pick up the non-mouse, non-human organisms 
        #    (as these are not included in MRK_Location_Cache)
        '''select distinct _Marker_key, genomicchromosome, chromosome,
                startcoordinate::varchar, endcoordinate::varchar,
                strand, cmoffset, cytogeneticoffset, version,
                _marker_type_key
            from mrk_location_cache
            union
            select  m._Marker_key, m.chromosome, m.chromosome, 
                null as startcoordinate, null as endcoordinate, null as strand,
                null as cmoffset, m.cytogeneticOffset, null as version, m._marker_type_key
            from mrk_marker m
            where _Marker_Status_key = 1
                  and cytogeneticOffset is not null
                  and not exists (select 1 from mrk_location_cache l
                        where m._Marker_key = l._Marker_key)''',

        # 3. all markers
        '''select _Marker_key, symbol, name, _Marker_Type_key, _Organism_key,
                _Marker_Status_key
        from mrk_marker''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Marker_key', 'symbol', 'name', 'markerType', 'subtype',
        'organism', 'accID', 'logicalDB', 'status', 'hasGOGraph',
        'location_display', 'coordinate_display', 'build_identifier', 'strain', 'strain_id', ]

# prefix for the filename of the output file
filenamePrefix = 'marker'

# global instance of a MarkerGatherer
gatherer = MarkerGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
