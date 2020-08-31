#!./python
# 
# gathers data for the 'image_alleles' table in the front-end database

import Gatherer
import logger

###--- Classes ---###

class ImageAllelesGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the image_alleles table
        # Has: queries to execute against the source database
        # Does: queries the source database for alleles associated with
        #       images, collates results, writes tab-delimited text file

        def collateResults (self):
                cols, rows = self.results[0]

                keyCol = Gatherer.columnNumber (cols, '_Allele_key')
                nameCol = Gatherer.columnNumber (cols, 'name')

                markerNames = {}
                for row in rows:
                        markerNames[row[keyCol]] = row[nameCol]
                logger.debug ('Cached marker names for %d alleles' % \
                        len(markerNames))

                self.finalColumns = [ '_Image_key', '_Allele_key', 'symbol',
                        'combinedName', 'accID', 'sequenceNum' ]
                self.finalResults = []

                cols, rows = self.results[1]

                alleleKeyCol = Gatherer.columnNumber (cols, '_Allele_key')
                imageKeyCol = Gatherer.columnNumber (cols, '_Image_key')
                thumbKeyCol = Gatherer.columnNumber (cols, '_ThumbnailImage_key')
                nameCol = Gatherer.columnNumber (cols, 'name')
                symbolCol = Gatherer.columnNumber (cols, 'symbol')
                idCol = Gatherer.columnNumber (cols, 'accID')

                i = 0
                for row in rows:
                        i = i + 1
                        alleleKey = row[alleleKeyCol]
                        thumbKey = row[thumbKeyCol]

                        # if we have a marker name for the allele, and if that
                        # marker name differs from the allele name, then
                        # prepend it

                        if alleleKey in markerNames and \
                            markerNames[alleleKey] != row[nameCol]:
                                name = markerNames[alleleKey] + '; ' + \
                                        row[nameCol]
                        else:
                                name = row[nameCol]

                        self.finalResults.append ( [ row[imageKeyCol],
                                alleleKey, row[symbolCol], name,
                                row[idCol], i ] )

                        i = i + 1
                        self.finalResults.append ( [ thumbKey,
                                alleleKey, row[symbolCol], name,
                                row[idCol], i ] )

                logger.debug ('Built %d output rows' % len(self.finalResults))
                return

###--- globals ---###

cmds = [
        # 0. cache marker names for every allele
        '''select distinct a._Allele_key, m.name
        from mrk_marker m, all_allele a
        where a._Marker_key = m._Marker_key''',

        # 1. main query
        '''select i._Image_key,
                a._Object_key as _Allele_key,
                aa.symbol,
                aa.name,
                acc.accID,
                i._ThumbnailImage_key
        from img_image i,
                img_imagepane p,
                img_imagepane_assoc a,
                all_allele aa,
                acc_accession acc
        where i._Image_key = p._Image_key
                and p._ImagePane_key = a._ImagePane_key
                and a._MGIType_key = 11
                and a._Object_key = aa._Allele_key
                and aa._Allele_key = acc._Object_key
                and acc.preferred = 1
                and acc._MGIType_key = 11
        order by i._Image_key, aa.symbol, aa.name'''
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        Gatherer.AUTO, '_Image_key', '_Allele_key', 'symbol', 'combinedName',
        'accID', 'sequenceNum',
        ]

# prefix for the filename of the output file
filenamePrefix = 'image_alleles'

# global instance of a ImageAllelesGatherer
gatherer = ImageAllelesGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
