#!./python
# 
# gathers data for the 'strain_mpd_data' table in the front-end database

import Gatherer
import logger
import StrainUtils
import subprocess
import config

###--- Globals ---###

mgiStrainName = {}                      # strain name (mixed case) : [ strain keys ]
mgiStrainNameLower = {}         # strain name (lowercase) : [ strain keys ]
mgiSynonym = {}                         # synonym (mixed case) : [ strain keys ]
mgiSynonymLower = {}            # synonym (lowercase) : [ strain keys ]
mgiAccID = {}                           # ID : [ strain keys ]

ldbOkay = [ 'Arc', 'Crl', 'Hsd', 'Ms', 'Nihon Clea, Tokyo', 'OHSU', 'Pas', 'Sanger', 'Unc', ]
ldbNeedsPrefix = []
ldbJax = 'JAX Registry'
ldbRiken = 'RIKEN BRC'
ldbTac = 'TAC'

###--- Functions ---###

def getMpdLines():
        # returns a list of data lines, processed from MPD.  Each line includes:
        #               (strain name, MPD ID, source, source ID, source URL)

        proc = subprocess.run('curl -L %s' % config.MPD_STRAIN_URL, shell=True, capture_output=True, encoding='utf-8')
        if (proc.returncode != 0):
                raise Exception('Failed to read from MPD')
        
        out = []
        for line in proc.stdout.split('\n'):
                # convert any commas between quotes to something else temporarily
                inQuote = False
                revisedLine = ''
                for c in line:
                        if c == '"':
                                inQuote = not inQuote
                                c = ''
                        elif c == ',' and inQuote:
                                c = '*COMMA*'
                        revisedLine = revisedLine + c

                # split into fields on commas
                fields = revisedLine.split(',')
                
                # convert the special comma flags back to commas
                row = [x.replace('*COMMA*', ',') for x in fields]

                if len(row) >= 10:
                        out.append( (row[0], row[4], row[1], row[2], row[9]) )
                elif len(row) >= 5:
                        out.append( (row[0], row[4], row[1], row[2], None) )
                else:
                        logger.debug('Skipped row (%d fields): "%s"' % (len(row), line))

        logger.debug('Got %d rows from MPD' % len(out))
        return out

def tweakID(accID, ldb):
        # adjust the 'accID' according to rules for the given 'ldb', since MGI and MPD store
        # IDs from the same logical database differently
        
        if ldb == ldbJax:
                if accID.startswith('JAX'):
                        accID = accID[3:]

        elif ldb == ldbRiken:
                if not accID.startswith('RBRC'):
                        return 'RBRC%s' % accID

        elif ldb == ldbTac:
                if accID.startswith('TAC:'):
                        return accID[4:]

        elif ldb in ldbNeedsPrefix:
                if not accID.startswith(ldb):
                        return '%s:%s' % (ldb, accID)
                
        return accID

###--- Classes ---###

class StrainMpdDataGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the strain_mpd_data table
        # Has: queries to execute against the source database
        # Does: queries the source database -- and MPD -- for MPD data about strains,
        #       collates results, writes tab-delimited text file

        def processIDs(self):
                # process accession IDs from the database, populating mgiAccIDs
                
                global mgiAccID
                
                cols, rows = self.results[2]
                keyCol = Gatherer.columnNumber(cols, '_Object_key')
                idCol = Gatherer.columnNumber(cols, 'accID')
                ldbCol = Gatherer.columnNumber(cols, 'name')
                
                for row in rows:
                        key = row[keyCol]
                        accID = tweakID(row[idCol], row[ldbCol])
                        
                        if accID not in mgiAccID:
                                mgiAccID[accID] = [ key ]
                        else:
                                mgiAccID[accID].append(key)
                                
                logger.debug('Processed %d ID rows' % len(rows))
                logger.debug('Got %d IDs' % len(mgiAccID))
                return
                                
        def processNames(self):
                # process strain names from the database, populating two global dictionaries
                
                global mgiStrainName, mgiStrainNameLower
                
                cols, rows = self.results[0]
                keyCol = Gatherer.columnNumber(cols, '_Strain_key')
                strainCol = Gatherer.columnNumber(cols, 'strain')
                
                for row in rows:
                        strain = row[strainCol]
                        key = row[keyCol]
                        strainLower = strain.lower()
                        
                        if strain not in mgiStrainName:
                                mgiStrainName[strain] = [ key ]
                        else:
                                mgiStrainName[strain].append(key)
                                
                        if strainLower not in mgiStrainNameLower:
                                mgiStrainNameLower[strainLower] = [ key ]
                        else:
                                mgiStrainNameLower[strainLower].append(key)
                                
                logger.debug('Processed %d name rows' % len(rows))
                logger.debug('Got %s strain names' % len(mgiStrainName))
                logger.debug('Got %s lowercase strain names' % len(mgiStrainNameLower))
                return 

        def processSynonyms(self):
                # process strain synonyms from the database, populating two global dictionaries
                
                global mgiSynonym, mgiSynonymLower
                
                cols, rows = self.results[1]
                keyCol = Gatherer.columnNumber(cols, '_Object_key')
                synonymCol = Gatherer.columnNumber(cols, 'synonym')
                
                for row in rows:
                        synonym = row[synonymCol]
                        key = row[keyCol]
                        synonymLower = synonym.lower()
                        
                        if synonym not in mgiSynonym:
                                mgiSynonym[synonym] = [ key ]
                        else:
                                mgiSynonym[synonym].append(key)
                                
                        if synonymLower not in mgiSynonymLower:
                                mgiSynonymLower[synonymLower] = [ key ]
                        else:
                                mgiSynonymLower[synonymLower].append(key)
                                
                logger.debug('Processed %d synonym rows' % len(rows))
                logger.debug('Got %s synonyms' % len(mgiSynonym))
                logger.debug('Got %s lowercase synonyms' % len(mgiSynonymLower))
                return 

        def collateResults(self):
                # In order to match up MPD data with MGI strain data, we need to do so by the
                # strain name stored in MPD.  This may be an MGI strain name, or a synonym, or
                # neither.
                
                mpdLines = getMpdLines()
                self.processNames()
                self.processSynonyms()
                self.processIDs()
                                
                # Now, we need to go through the data lines from MPD.  For each...
                # 0. If we can match by ID, use that strain key.
                # 1. If not #0, if the MPD strain name matches an MGI strain name, use that strain key.
                # 2. If not #1, if the MPD strain name matches an MGI synonym, use that strain key.
                # 3. If not #2, if the MPD strain name matches (case-insensitive) an MGI strain name, use that strain key.
                # 4. If not #3, if the MPD strain name matches (case-insensitive) an MGI synonym, use that strain key.

                self.finalColumns = [ 'strain_key', 'mpd_id', 'repository', 'source_url', 'match_type', 'mpd_strain' ]
                self.finalResults = []
                
                noMatchCt = 0
                
                matchType = None
                for (mpdStrain, mpdID, source, sourceID, sourceURL) in mpdLines:
                        strainKeys = []
                        if sourceID in mgiAccID:
                                strainKeys = mgiAccID[sourceID]
                                matchType = 'exact match to ID'
                        elif mpdStrain in mgiStrainName:
                                strainKeys = mgiStrainName[mpdStrain]
                                matchType = 'exact match to name'
                        elif mpdStrain in mgiSynonym:
                                strainKeys = mgiSynonym[mpdStrain]
                                matchType = 'exact match to synonym'
                        else:
                                mpdStrainLower = mpdStrain.lower()
                                if mpdStrainLower in mgiStrainNameLower:
                                        strainKeys = mgiStrainNameLower[mpdStrainLower]
                                        matchType = 'case-insensitive match to name'
                                elif mpdStrainLower in mgiSynonymLower:
                                        strainKeys = mgiSynonymLower[mpdStrainLower]
                                        matchType = 'case-insensitive match to synonym'
                                else:
                                        noMatchCt = noMatchCt + 1
                                        
                        # hack to ensure Python 3.7 version produces exactly the same data as Python 2.4
                        # (the 2.4 version had a space at the end of every URL)
                        if (sourceURL == None):
                            sourceURL = ''
                        sourceURL = sourceURL + ' ' 

                        for strainKey in strainKeys:
                                self.finalResults.append( [ strainKey, mpdID, source, sourceURL, matchType, mpdStrain ] )

                logger.debug('%d MPD strain IDs and names did not match' % noMatchCt)
                logger.info('Produced %d output rows' % len(self.finalResults))
                return
        
###--- globals ---###

cmds = [
        # 0. get the strain names themselves
        '''select s._Strain_key, s.strain
                from prb_strain s, %s t
                where s._Strain_key = t._Strain_key''' % StrainUtils.getStrainTempTable(),

        # 1. get the synonyms for the strains
        '''select s._Object_key, s.synonym
                from mgi_synonym s, %s z, mgi_synonymtype t
                where s._Object_key = z._Strain_key
                        and s._SynonymType_key = t._SynonymType_key
                        and t._MGIType_key = 10''' % StrainUtils.getStrainTempTable(),
                        
        # 2. non-MGI strain IDs (skip MGI, because MPD has no MGI strain IDs)
        '''select a._Object_key, ldb.name, a.accID
                from acc_accession a, acc_logicaldb ldb, %s t
                where ldb.name != 'MGI'
                        and a._Object_key = t._Strain_key
                        and a._LogicalDB_key = ldb._LogicalDB_key
                        and a._MGIType_key = 10''' % StrainUtils.getStrainTempTable(),
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        Gatherer.AUTO, 'strain_key', 'mpd_id', 'repository', 'source_url', 'match_type', 'mpd_strain',
        ]

# prefix for the filename of the output file
filenamePrefix = 'strain_mpd_data'

# global instance of a StrainMpdData Gatherer
gatherer = StrainMpdDataGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
