# Module: MMHCdbData.py
# Purpose: to access allele and marker counts from MMHCdb

import logger
import urllib.request, urllib.error, urllib.parse
import config
import gc

class MMHCdbDatabase:
        #FIXME: MMHCDB_ALLELE_URL = config.MMHCDB_ALLELE_URL
        MMHCDB_ALLELE_URL = 'http://tumor.informatics.jax.org/mtbwi/alleleIDReport.do'
        # Returns allele IDs that have tumor data in MMHCdb
        def getAlleleIDs (self):
                with urllib.request.urlopen(self.MMHCDB_ALLELE_URL) as f:
                    contents = f.read()
                    alleleIDs = contents.decode('utf-8').strip().split()
                return alleleIDs
