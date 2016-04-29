#!/usr/local/bin/python
# 
# gathers data for the 'marker_qtl_experiments' table in the front-end database

import Gatherer
import logger

###--- Constants ---###
#       Approx length of the Y chromosome, per Janan. 
#       Necessary because the highest assigned markers are at
#       1 cM, which would cause a very odd map to be generated. 
MAX_YCM_OFFSET = 47.0

###--- Classes ---###

class MarkerMinimapGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the marker_minimap_marker table
        
        def getAnchorMarkerMap(self):
                """
                map of anchor markers in format
                        { 
                          chromosome: [{
                                  _marker_key,
                                  symbol,
                                  cmoffset
                            },...
                          ],...
                        }
                """
                
                cols, rows = self.results[0]
                
                markerKeyCol = Gatherer.columnNumber (cols, '_marker_key')
                symbolCol = Gatherer.columnNumber (cols, 'symbol')
                chrCol = Gatherer.columnNumber (cols, 'chromosome')
                cmOffsetCol = Gatherer.columnNumber (cols, 'cmoffset')
                
                anchorMap = {}
                
                for row in rows:
                        markerKey = row[markerKeyCol]
                        symbol = row[symbolCol]
                        chr = row[chrCol]
                        cmOffset = row[cmOffsetCol]
                        
                        anchorMap.setdefault(chr, []) \
                            .append({
                                     '_marker_key': markerKey,
                                     'symbol': symbol,
                                     'cmoffset': cmOffset
                             })
                        
                return anchorMap
        
        
        def getMaxCmMap(self):
                """
                Get map of chr to max cm offset
                """
                maxCmMap = {}
                
                cols, rows = self.results[1]
                maxCmCol = Gatherer.columnNumber (cols, 'maxoffset')
                chrCol = Gatherer.columnNumber (cols, 'chromosome')
                
                for row in rows:
                        maxCmOffset = row[maxCmCol]
                        chr = row[chrCol]
                        
                        if chr == 'Y':
                                maxCmOffset = MAX_YCM_OFFSET
                                
                        maxCmMap[chr] = maxCmOffset
                
                return maxCmMap
        

	def collateResults (self):

                # get anchor markers for each chromosome
                anchorMap = self.getAnchorMarkerMap()
                
                # get max cm offset for each chromosome
                maxCmMap = self.getMaxCmMap()
                
                
                # process all mouse markers with cm offsets
                cols, rows = self.results[2]
                
                markerKeyCol = Gatherer.columnNumber (cols, '_marker_key')
                symbolCol = Gatherer.columnNumber (cols, 'symbol')
                chrCol = Gatherer.columnNumber (cols, 'chromosome')
                cmOffsetCol = Gatherer.columnNumber (cols, 'cmoffset')
                
                markerRows = []
                
                for row in rows:
                    markerKey = row[markerKeyCol]
                    symbol = row[symbolCol]
                    chr = row[chrCol]
                    cmOffset = row[cmOffsetCol]
                        
                    if chr in anchorMap and chr in maxCmMap:
                                
                        maxCmOffset = maxCmMap[chr]
                        
                        markerRows.append(
                         (markerKey, 
                          markerKey,
                          symbol,
                          cmOffset,
                          maxCmOffset)
                        )
                        
                        for anchor in anchorMap[chr]:
                                
                            # skip if primary is also an anchor
                            if anchor['_marker_key'] == markerKey:
                                    continue
                            
                            markerRows.append(
                             (markerKey, 
                              anchor['_marker_key'],
                              anchor['symbol'],
                              anchor['cmoffset'],
                              maxCmOffset)
                            )
                                
                
                self.finalColumns = [ 'marker_key', 
                                     'anchor_marker_key', 
                                     'anchor_symbol', 
                                     'cm_offset', 
                                     'max_cm_offset' ]

		self.finalResults = markerRows
                

###--- globals ---###

cmds = [
        # 0. Anchor markers and their cm offsets
	'''select m._marker_key,
	        m.symbol, 
                m.chromosome, 
                o.cmoffset
        from mrk_marker m 
        join mrk_offset o on 
                o._marker_key = m._marker_key
        join mrk_anchors a on
                a._marker_key = m._marker_key
         where 
                -- MGD source
                o.source = 0
                -- has valid CM
                and o.cmoffset > -1.0
	''',
        
        # 1. get max cm offset for each chromosome
        '''select max(o.cmoffset) as maxoffset,
                m.chromosome
        from mrk_marker m
        join mrk_offset o on
                o._marker_key = m._marker_key
        where 
                -- MGD source
                o.source = 0
                -- is mouse
                and m._organism_key = 1
        group by chromosome
        ''',
        
        # 2. All markers with cm coordinates
        '''select m._marker_key,
                m.symbol,
                m.chromosome,
                o.cmoffset
        from mrk_marker m
        join mrk_offset o on
                o._marker_key = m._marker_key
        where 
                -- MGD source
                o.source = 0
                -- has valid CM
                and o.cmoffset > -1.0
                -- is mouse
                and m._organism_key = 1
        '''
        
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, 'marker_key', 'anchor_marker_key', 'anchor_symbol', 'cm_offset', 'max_cm_offset',
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_minimap_marker'

# global instance of a MarkerQtlExperimentsGatherer
gatherer = MarkerMinimapGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
