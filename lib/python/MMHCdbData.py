# Module: MMHCdbData.py
# Purpose: to access allele and marker counts from MMHCdb

import logger
import urllib.request, urllib.error, urllib.parse
import config
import gc
import ssl

class MMHCdbDatabase:
        # Returns allele IDs that have tumor data in MMHCdb
        def getAlleleIDs (self):

            # adding this will cause the request to ignore any cert errors
            myssl_context = ssl._create_unverified_context()

            with urllib.request.urlopen(config.MMHCDB_ALLELE_URL, context=myssl_context) as f:
                contents = f.read()
                alleleIDs = contents.decode('utf-8').strip().split()
            return alleleIDs
