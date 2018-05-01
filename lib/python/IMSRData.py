# Module: IMSRData.py
# Purpose: to access allele and marker counts from IMSR

import logger
import urllib2
import config
import gc

class IMSRDatabase:
	IMSR_COUNT_URL = 'IMSR_COUNT_URL' in dir(config) and config.IMSR_COUNT_URL or ""
	IMSR_COUNT_TIMEOUT = 'IMSR_COUNT_TIMEOUT' in dir(config) and config.IMSR_COUNT_TIMEOUT or ""

	# Issues a Solr query to IMSR_URL to retrieve 
	# cellLineCount, strainCount, totalCountForMarker
	# 	Return dictionaries keyed by accession ID
	def queryAllCounts (self):
		logger.debug ('IMSR_COUNT_URL : %s' % self.IMSR_COUNT_URL)
		logger.debug ('IMSR_COUNT_TIMEOUT : %d' % self.IMSR_COUNT_TIMEOUT)

		# TODO (kstone): add timeout when we move past python 2.4
		#	Not supported in older versions
		#f = urllib2.urlopen(self.IMSR_COUNT_URL, timeout=self.IMSR_COUNT_TIMEOUT)
		f = urllib2.urlopen(self.IMSR_COUNT_URL)
		try:
		    lines = f.readlines()
		finally:
		    f.close()
		

		cellLines = {}
		strains = {}
		byMarker = {}

		if not lines:
			logger.error ('Error reading from IMSR_COUNT_URL: %s' % err)
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
	IMSR_STRAIN_URL = 'IMSR_STRAIN_URL' in dir(config) and config.IMSR_STRAIN_URL or ""

	# Issues a query to the IMSR WI to retrieve a report mapping from mouse strain names to
	# the IDs IMSR recognizes for them (and other data, too).  This report is fairly big (125 MB).
	# cellLineCount, strainCount, totalCountForMarker
	# Returns: a list of tuples, each including:
	#		(strain name or synonym, approved nomen flag (0/1), IMSR ID, repository, source URL)
	def queryStrains (self):
		logger.debug ('IMSR_STRAIN_URL : %s' % self.IMSR_STRAIN_URL)

		f = urllib2.urlopen(self.IMSR_STRAIN_URL)
		try:
		    lines = f.readlines()
		finally:
		    f.close()
		
		out = []
		uniqueSet = set()

		if not lines:
			logger.error ('Error reading from IMSR_STRAIN_URL: %s' % err)
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
	imsrDB.IMSR_COUNT_URL = "http://www.findmice.org/report/mgiCounts.txt"
	imsrDB.IMSR_COUNT_TIMEOUT=300
	imsrStrains = IMSRStrainData()
	import unittest
	class IMSRDBTestCase(unittest.TestCase):
		def test_queryAllCounts(self):
			cellLineCount,strainCount,markerCount = imsrDB.queryAllCounts()
			print "count of cell lines = %d"%len(cellLineCount)
			print "count of strains = %d"%len(strainCount)
			print "count of markers = %d"%len(markerCount)
			self.assertTrue(markerCount["MGI:97490"] > 0,"Pax6 count not greater than zero")
			print "pax6 count = %s"%markerCount["MGI:97490"]
	class IMSRStrainTestCase(unittest.TestCase):
		def test_queryStrains(self):
			lines = imsrStrains.queryStrains()
			self.assertTrue(len(lines) > 0, 'Failed to retrieve lines')
			print 'Got %d strain lines' % len(lines)
	unittest.main()
