#!./python
# 
# gathers data for the 'marker_wksilvers' table in the front-end database

import Gatherer

###--- Classes ---###

WKSilversGatherer = Gatherer.Gatherer
        # Is: a data gatherer for the marker_wksilvers table
        # Has: queries to execute against the source database
        # Does: queries the source database for marker data cited in book by W.K. Silvers,
        #       collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
        '''select k._rosetta_key, k.wks_markersymbol, k.wks_markerurl, m._Marker_key, m.symbol, a.accID
        from wks_rosetta k, mrk_marker m, acc_accession a
        where k._Marker_key = m._Marker_key
                and k._Marker_key = a._Object_key
                and a._MGIType_key = 2
                and a._LogicalDB_key = 1
                and a.preferred = 1
        order by 1'''
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        '_rosetta_key', 'wks_markersymbol', 'wks_markerurl', '_marker_key', 'symbol', 'accID', '_rosetta_key',
        ]

# prefix for the filename of the output file
filenamePrefix = 'marker_wksilvers'

# global instance of a WKSilversGatherer
gatherer = WKSilversGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
