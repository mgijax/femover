#!./python
# 
# gathers data for the 'image_sequence_num' table in the front-end database

import Gatherer
import logger

###--- Classes ---###

class ImageSequenceNumGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the image_sequence_num table
        # Has: queries to execute against the source database
        # Does: queries the source database for sorting data for images,
        #       collates results, writes tab-delimited text file

        def getReferenceSortValues (self):
                # collect reference info

                cols, rows = self.results[0]

                keyCol = Gatherer.columnNumber (cols, '_Refs_key')
                yearCol = Gatherer.columnNumber (cols, 'year')
                jnumCol = Gatherer.columnNumber (cols, 'numericPart')

                self.jnum = {}

                gxdStyleList = []
                phenoStyleList = []

                for row in rows:
                        key = row[keyCol]
                        year = row[yearCol]
                        jnum = row[jnumCol]

                        self.jnum[key] = jnum

                        # year descending, jnum ascending
                        gxdStyleList.append ( (-year, jnum, key) )

                        # prioritize the odd (J:98862) reference:
                        if jnum == 98862:
                                jnum = 999999999
                        phenoStyleList.append ( (jnum, key) )

                gxdStyleList.sort()
                phenoStyleList.sort()
                phenoStyleList.reverse()

                gxdRefSortVal = {}
                phenoRefSortVal = {}

                i = 0
                for (year, jnum, key) in gxdStyleList:
                        i = i + 1
                        gxdRefSortVal[key] = i

                j = 0
                for (jnum, key) in phenoStyleList:
                        j = j + 1
                        phenoRefSortVal[key] = j

                logger.debug ('Got sortable reference values')
                return gxdRefSortVal, phenoRefSortVal

        def collectImages (self, resultIndex, name):
                cols, rows = self.results[resultIndex]

                self.imageToRef = {}

                imageCol = Gatherer.columnNumber (cols, '_Image_key')
                refsCol = Gatherer.columnNumber (cols, '_Refs_key')
                labelCol = Gatherer.columnNumber (cols, 'figureLabel')

                # image key -> (image key, refs key, figure label)
                images = {}

                for row in rows:
                        # switch to lowercase and replace underscores with
                        # spaces for proper sorting
                        images[row[imageCol]] = (row[imageCol], row[refsCol],
                                row[labelCol].lower().replace('_', ' '))

                        self.imageToRef[row[imageCol]] = row[refsCol]

                logger.debug ('Got %d %s images' % (len(images), name))
                return images

        def collateResults (self):
                # We sort images by the following rules:
                # 1. GXD images first
                #       a. images with only in situ assays
                #       b. images with both in situ and gel assays
                #       c. images with only gel assays
                #       d. within each category (a-c), sort by:
                #               year, J: numeric part, figure label, image key
                # 2. phenotype images second
                #       a. images from J:98862 first (refs key 99888)
                #       b. images from other J: numbers
                #       c. within each category (a-b), sort by:
                #               J: numeric part, figure label, image key
                # 3. molecular images third:
                #       a. by J: numeric part, figure label, image key

                # each dict is:
                #       refs key -> sort value
                gxdRefSortVal, phenoRefSortVal = self.getReferenceSortValues()

                # each dict is:
                #       image key -> (image key, refs key, figure label)
                inSituImages = self.collectImages(1, 'in situ')
                gelImages = self.collectImages (2, 'gel')
                phenoImages = self.collectImages(3, 'pheno')
                otherImages = self.collectImages(4, '(all)')

                # go through and remove from 'otherImages' any images which
                # are in one of the other sets

                allKeys = list(otherImages.keys())
                allKeys.sort()
                for key in allKeys:
                        if key in inSituImages or \
                                key in gelImages or \
                                key in phenoImages:
                                        del otherImages[key]
                logger.debug ('Weeded down to %d other images' % \
                        len(otherImages))

                # collate a list of images with both in situ and gel assays,
                # and remove those images from the two other lists

                bothGxd = {}
                inSituKeys = list(inSituImages.keys())
                inSituKeys.sort()
                for key in inSituKeys:
                        if key in gelImages:
                                bothGxd[key] = gelImages[key]
                                del gelImages[key]
                                del inSituImages[key]

                logger.debug ('Found %d with both gel and in situ' % \
                        len(bothGxd))

                # produce a sortable list of gxd image data, with each as:
                # (group number, reference sort val, figure label, image key)

                group = 0
                sortableList = []
                for d in [ inSituImages, bothGxd, gelImages ]:
                        group = group + 1
                        toSort = list(d.items())
                        toSort.sort()
                        for (key, (image, refs, figureLabel)) in toSort:
                                sortableList.append ( [group,
                                        gxdRefSortVal[refs], figureLabel,
                                        image] )

                # add sortable pheno and other image data, with each as:
                # (group number, reference sort val, figure label, image key)
                for d in [ phenoImages, otherImages ]:
                        group = group + 1
                        toSort = list(d.items())
                        toSort.sort()
                        for (key, (image, refs, figureLabel)) in toSort:
                                sortableList.append ( [group,
                                        phenoRefSortVal[refs], figureLabel,
                                        image] )

                sortableList.sort()
                logger.debug ('Sorted %d images' % len(sortableList))

                # product final set of results:

                self.finalColumns = [ '_Image_key', 'sortVal', 'numericPart' ]
                self.finalResults = []

                i = 0
                for [group, refsVal, figureLabel, imageKey] in sortableList:
                        refs = self.imageToRef[imageKey]
                        jnum = self.jnum[refs]

                        i = i + 1
                        self.finalResults.append ( [ imageKey, i, jnum ] )
                return

###--- globals ---###

cmds = [
        # 0. get sorting info for references first
        '''select c._Refs_key, b.year, c.numericPart
        from bib_citation_cache c, bib_refs b, img_image i
        where c._Refs_key = b._Refs_key
                and c._Refs_key = i._Refs_key''',

        # 1. get GXD images with in situ assays
        '''select i._Image_key, i._Refs_key, i.figureLabel
        from img_image i,
                img_imagepane p,
                gxd_insituresultimage g
        where i._Image_key = p._Image_key
                and p._ImagePane_key = g._ImagePane_key''', 

        # 2. get GXD images with gel assays (image associated directly to
        # assay)
        '''select i._Image_key, i._Refs_key, i.figureLabel
        from img_image i,
                img_imagepane p,
                gxd_assay a
        where exists (select 1 from GXD_Expression e where a._Assay_key = e._Assay_key)
                and i._Image_key = p._Image_key
                and p._ImagePane_key = a._ImagePane_key''',

        # 3. get phenotype images
        '''select i._Image_key, i._Refs_key, i.figureLabel
        from img_image i,
                img_imagepane p,
                img_imagepane_assoc a
        where i._Image_key = p._Image_key
                and p._ImagePane_key = a._ImagePane_key
                and a._MGIType_key = 11''',

        # 4. get all images (so we can catch any outliers)
        '''select i._Image_key, i._Refs_key, i.figureLabel
        from img_image i''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Image_key', 'sortVal', 'numericPart' ]

# prefix for the filename of the output file
filenamePrefix = 'image_sequence_num'

# global instance of a ImageSequenceNumGatherer
gatherer = ImageSequenceNumGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
