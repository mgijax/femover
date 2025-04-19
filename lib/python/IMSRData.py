# Module: IMSRData.py
# Purpose: to access allele and marker counts from IMSR

import logger
import urllib.request, urllib.error, urllib.parse
import config
import gc

class IMSRDatabase:
        IMSR_COUNT_FILE = 'IMSR_COUNT_FILE' in dir(config) and config.IMSR_COUNT_FILE or ""

        def queryAllCounts (self):
                logger.debug ('IMSR_COUNT_FILE : %s' % self.IMSR_COUNT_FILE)

                cellLines = {}
                strains = {}
                byMarker = {}

                try:
                    f = open(self.IMSR_COUNT_FILE, 'rb')
                except:
                    logger.error ('Failed to open IMSR_COUNT_FILE: %s' % self.IMSR_COUNT_FILE)
                    return cellLines, strains, byMarker

                try:
                    lines = f.readlines()
                finally:
                    f.close()
                
                if len(lines) == 0:
                    logger.error ('Failed to read records from IMSR_COUNT_FILE: %s' % self.IMSR_COUNT_FILE)
                    return cellLines, strains, byMarker

                if type(lines) == bytes:
                        lines = 'n'.split(lines.decode(errors="replace"))
                elif type(lines[0]) == bytes:
                        lines = [x.decode(errors="replace") for x in lines]

                if not lines:
                        logger.error ('Error reading from IMSR_COUNT_FILE: %s' % err)
                        logger.error ('No counts will be stored')

                        raise Exception ('Could not retrieve data from IMSR')
                        return cellLines, strains, byMarker
                        
                for line in lines:
                        items = line.split()

                        # skip blank lines
                        if len(line.strip()) == 0:
                                continue

                        # report (and skip) lines with too few fields; this would
                        # indicate a bug in IMSR
                        if len(items) < 3:
                                logger.debug (
                                        'Line from IMSR has too few fields: %s' % \
                                        line)
                                continue

                        # look for the three tags we need (other counts for KOMP are
                        # included in the same report, so we skip any we don't need)

                        accID = items[0]
                        countType = items[1]
                        count = items[2]

                        if countType == 'ALL:ES':
                                cellLines[accID] = count
                        elif countType == 'ALL:ST':
                                strains[accID] = count
                        elif countType == 'MRK:UN':
                                byMarker[accID] = count

                logger.debug ('Cell lines: %d, Strains: %d, byMarker: %d' % (
                        len(cellLines), len(strains), len(byMarker) ) )

                return cellLines, strains, byMarker

class IMSRStrainData:
        IMSR_STRAIN_FILE = 'IMSR_STRAIN_FILE' in dir(config) and config.IMSR_STRAIN_FILE or ""

        # Issues a query to the IMSR WI to retrieve a report mapping from mouse strain names to
        # the IDs IMSR recognizes for them (and other data, too).  This report is fairly big (125 MB).
        # cellLineCount, strainCount, totalCountForMarker
        # Returns: a list of tuples, each including:
        #               (strain name or synonym, approved nomen flag (0/1), IMSR ID, repository, source URL)
        def queryStrains (self):
                logger.debug ('Getting IMSR strain data from: %s' % self.IMSR_STRAIN_FILE)

                f = open(self.IMSR_STRAIN_FILE, 'rb')
                try:
                    lines = f.readlines()
                finally:
                    f.close()
                
                if type(lines) == bytes:
                        lines = 'n'.split(lines.decode(errors="replace"))
                if type(lines[0]) == bytes:
                        lines = [ x.decode('utf-8', errors='replace') for x in lines ]

                out = []
                uniqueSet = set()

                if not lines:
                        logger.error ('Error reading from IMSR_STRAIN_FILE: %s' % err)
                        logger.error ('No IMSR strain data will be stored')

                        raise Exception ('Could not retrieve strain data from IMSR')
                        return out
                        
                for line in lines:
                        # skip blank lines
                        if len(line.strip()) == 0:
                                continue

                        items = line.split('\t')

                        # report (and skip) lines with too few fields; this would
                        # indicate a bug in IMSR
                        if len(items) < 14:
                                logger.debug ('Line from IMSR has too few fields (%d): %s' % (len(items), line))
                                continue

                        # extract needed data
                        nomenFlag = 0
                        if items[0] == '+':
                                nomenFlag = 1
                        
                        [ imsrID, strain, repository ] = items[1:4]
                        url = items[13]
                        
                        key = (imsrID, strain, repository)
                        if key not in uniqueSet:
                                uniqueSet.add(key)
                                out.append( (strain, nomenFlag, imsrID, repository, url) )
                        
                logger.debug ('Condensed %d lines down to %d' % (len(lines), len(out)))
                del lines
                gc.collect()
                        
                return out

if __name__=="__main__":
        imsrDB = IMSRDatabase()
        imsrDB.IMSR_COUNT_FILE = "https://www.findmice.org/report/mgiCounts.txt"
        imsrStrains = IMSRStrainData()
        import unittest
        class IMSRDBTestCase(unittest.TestCase):
                def test_queryAllCounts(self):
                        cellLineCount,strainCount,markerCount = imsrDB.queryAllCounts()
                        print(("count of cell lines = %d"%len(cellLineCount)))
                        print(("count of strains = %d"%len(strainCount)))
                        print(("count of markers = %d"%len(markerCount)))
                        self.assertTrue(markerCount["MGI:97490"] > 0,"Pax6 count not greater than zero")
                        print(("pax6 count = %s"%markerCount["MGI:97490"]))
        class IMSRStrainTestCase(unittest.TestCase):
                def test_queryStrains(self):
                        lines = imsrStrains.queryStrains()
                        self.assertTrue(len(lines) > 0, 'Failed to retrieve lines')
                        print(('Got %d strain lines' % len(lines)))
        unittest.main()
