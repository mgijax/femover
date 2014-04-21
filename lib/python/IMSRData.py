# Module: IMSRData.py
# Purpose: to access allele and marker counts from IMSR

import logger
import httpReader
import config

class IMSRDatabase:
	IMSR_COUNT_URL = 'IMSR_COUNT_URL' in dir(config) and config.IMSR_COUNT_URL or ""
	IMSR_COUNT_TIMEOUT = 'IMSR_COUNT_TIMEOUT' in dir(config) and config.IMSR_COUNT_TIMEOUT or ""

	# Issues a Solr query to IMSR_URL to retrieve 
	# cellLineCount, strainCount, totalCountForMarker
	# 	Return dictionaries keyed by accession ID
	def queryAllCounts (self):
		logger.debug ('IMSR_COUNT_URL : %s' % self.IMSR_COUNT_URL)
		logger.debug ('IMSR_COUNT_TIMEOUT : %d' % self.IMSR_COUNT_TIMEOUT)

		(lines, err) = httpReader.getURL (self.IMSR_COUNT_URL,
			timeout = self.IMSR_COUNT_TIMEOUT)

		cellLines = {}
		strains = {}
		byMarker = {}

		if not lines:
			logger.error ('Error reading from IMSR_COUNT_URL: %s' % err)
			logger.error ('No counts will be stored')

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


if __name__=="__main__":
	imsrDB = IMSRDatabase()
	imsrDB.IMSR_COUNT_URL = "http://emnet.informatics.jax.org:48080/imsrwi/imsrwi/report/mgiCounts.txt"
	imsrDB.IMSR_COUNT_TIMEOUT=300
	import unittest
	class IMSRDBTestCase(unittest.TestCase):
		def test_queryAllCounts(self):
			cellLineCount,strainCount,markerCount = imsrDB.queryAllCounts()
			print len(cellLineCount)
			print len(strainCount)
			print len(markerCount)
			self.assertTrue(markerCount["MGI:97490"] > 0,"Pax6 count not greater than zero")
	unittest.main()
