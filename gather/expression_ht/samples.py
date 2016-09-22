# Name: expression_ht/experiments.py
# Purpose: provides functions for dealing with experiment retrieval across multiple expression_ht* gatherers

import dbAgnostic
import logger
import constants as C
from gc import collect
from experiments import getRowCount, getExperimentTempTable, getExperimentKey

###--- Globals ---###

sampleTable = None

###--- Public Functions ---###

def getSampleTempTable():
    # Get the name of a temp table that contains the samples we need to move to the front-end database.
    
    global sampleTable

    if sampleTable:             # already built the table?  If so, just return the name.
        return sampleTable
    
    sampleTable = 'gxdht_samples_to_move'
    cmd0 = '''create temp table %s (
                _Sample_key        int    not null,
                _Experiment_key    int    not null
                )''' % sampleTable
                
    cmd1 = '''insert into %s
                select s._Sample_key, e._Experiment_key
                from %s e, gxd_htsample s
                where e._Experiment_key = s._Experiment_key''' % (sampleTable, getExperimentTempTable())
                
    cmd2 = 'create unique index gets1 on %s (_Sample_key)' % sampleTable
    cmd3 = 'create unique index gets2 on %s (_Experiment_key)' % getExperimentTempTable()
    
    dbAgnostic.execute(cmd0)
    logger.debug('Created temp table %s' % sampleTable)
    
    dbAgnostic.execute(cmd1)
    logger.debug('Populated %s with %d rows' % (sampleTable, getRowCount(sampleTable)))
    
    dbAgnostic.execute(cmd2)
    dbAgnostic.execute(cmd3)
    logger.debug('Indexed %s' % sampleTable)
    return sampleTable

def getSampleCountsByExperiment():
    # Get a dictionary mapping from each experiment key to the count of its samples (for experiments
    # that are to be moved into the front-end database).
    
    cmd0 = 'select _Experiment_key from %s' % getExperimentTempTable()
    cmd1 = '''select _Experiment_key, count(1) as ct 
                from %s
                group by _Experiment_key''' % getSampleTempTable()
#    cmd1 = '''select t._Experiment_key, count(distinct f.sample_id) as ct 
#                from %s t, acc_accession a, gxd_htsample_fields f
#                where t._Experiment_key = a._Object_key
#                    and a._MGIType_key = 42
#                    and a.preferred = 1
#                    and a.accID = f.experiment_id
#                group by t._Experiment_key''' % getExperimentTempTable()
                
    counts = {}

    # We initialize all experiment counts to zero first, just in case there are some experiments
    # without samples.  Then we come back and overwrite the counts for those with samples.
    
    cols, rows = dbAgnostic.execute(cmd0)
    for row in rows:
        counts[row[0]] = 0
    logger.debug('Initialized sample counts for %d experiments' % len(rows))
    
    cols, rows = dbAgnostic.execute(cmd1)
    exptKeyCol = dbAgnostic.columnNumber(cols, '_Experiment_key')
    countCol = dbAgnostic.columnNumber(cols, 'ct')
    
    for row in rows:
        counts[row[exptKeyCol]] = row[countCol]
    logger.debug('Got final sample counts for %d experiments' % len(rows))
    
    # TODO - remove this hack once we have curated sample data
    ct = 0
    for experimentID in getExperimentsWithSamples():
        experimentKey = getExperimentKey(experimentID)
        if experimentKey:
            ct = ct + 1
            counts[experimentKey] = getSampleCount(experimentID)
    logger.debug('Hacked %d sample counts' % ct)

    return counts

###--- temporary code for pulling samples from data file in project directory ---###
###--- TODO : remove this section ---###

sampleFile = None

def _loadSampleFile():
    global sampleFile
    if not sampleFile:
        sampleFile = SampleFile()
        logger.debug("Initialized sampleFile")
    return
    
def gc():
    collect()

def getSamples(experimentID):
    _loadSampleFile()
    return sampleFile.getSamples(experimentID)

def getSampleCount(experimentID):
    return len(getSamples(experimentID))
    
def getExperimentsWithSamples():
    _loadSampleFile()
    return sampleFile.getExperimentIDs() 

class SampleFile:
    def __init__ (self):
        self.samples = {}                           # experiment ID : [ samples ]
        self.s = self.readFile()
        self.populateExperiments()
        return
        
    def getSamples(self, experimentID):
        if experimentID in self.samples:
            return self.samples[experimentID]
        return []
    
    def getExperimentIDs(self):
        return self.samples.keys()

    def populateExperiments(self):
        lastGcSize = len(self.s)
        while self.s:
            experiment, self.s = self.popExperiment(self.s)
            if not experiment:
                break

            if 'experiment' in experiment:
                if 'accession' in experiment['experiment']:
                    experimentID = experiment['experiment']['accession']

                if experimentID and ('sample' in experiment['experiment']):
                    self.samples[experimentID] = experiment['experiment']['sample']

            # if we can reclaim 1MB of memory, do it
            if (lastGcSize - len(self.s)) >= 1000000:
                gc()
                lastGcSize = len(self.s)
            
            # report periodically
            if len(self.samples) % 100 == 0:
                logger.debug('Got experiment %d, len(s) = %d' % (len(self.samples), len(self.s)))
                
        logger.debug("Collected samples for %d experiments" % len(self.samples))
        return
    
    def readFile(self):
        fp = open('/mgi/all/wts_projects/12300/12370/ArrayExpressData/samples.json', 'r')
        lines = fp.readlines() 
        fp.close()
        logger.debug('Read data file')

        lines = map(lambda s: s.strip(), lines)
        logger.debug('Removed leading and trailing whitespace')

        s = ''.join(lines).strip()
        logger.debug('Joined lines into one string')

        del lines
        gc()

        errors = 0
        startTag = '<!doctype html>'
        endTag = '</html>'
        start = s.find(startTag)
        while start >= 0:
            stop = s.find(endTag, start)
            if stop >= 0:
                s = s[:start] + s[stop + len(endTag) + 1:]
                errors = errors + 1
            start = s.find(startTag, start)

        logger.debug('Removed %d error reports' % errors)
        gc()

        s = s.replace('null', 'None').replace('\\/', '/')
        logger.debug('Did string replacement(s)')

        gc()
        logger.debug('Returning input string, length %d' % len(s))
        return s
    
    def popExperiment(self, s):
        # pop the first experiment off string s
        # returns (experiment, remainder of s)
        
        if not s:
            return None, None
        start = s.find('{')
        if not start:
            return None, None
        openCount = 1
        nextPos = start + 1
        halt = False

        while not halt:
            openBracket = s.find('{', nextPos)
            closeBracket = s.find('}', nextPos)
            quote = s.find('"', nextPos)

            if quote < openBracket and quote < closeBracket:
                closeQuote = s.find('"', quote + 1)
                nextPos = closeQuote + 1

            elif openBracket > start:
                if openBracket < closeBracket:
                    openCount = openCount + 1
                    nextPos = openBracket + 1
                else:
                    openCount = openCount - 1
                    nextPos = closeBracket + 1
                    if openCount == 0:
                        halt = True

            elif closeBracket > start:
                openCount = openCount - 1
                nextPost = closeBracket + 1
                halt = True

        if openCount == 0 and start != nextPos:
            try:
                experiment = eval(s[start:nextPos])
                s = s[nextPos:]
            except SyntaxError:
                logger.debug('Failed experiment: %s' % s[start:nextPos])
                return None, None
        else:
            return None, s[nextPos:]

        return experiment, s

emapaCache = {}     # term : key
startStage = {}     # term key : start stage
def getEmapa(structure):
    global emapaCache, startStage

    if not emapaCache:
        cols, rows = dbAgnostic.execute('''select t.term, t._Term_key, a.startStage
            from voc_term t, voc_vocab v, voc_term_emapa a
            where t._Vocab_key = v._Vocab_key
            and t._Term_key = a._Term_key
            and v.name = 'EMAPA' ''')
        
        termCol = dbAgnostic.columnNumber(cols, 'term')
        keyCol = dbAgnostic.columnNumber(cols, '_Term_key')
        stageCol = dbAgnostic.columnNumber(cols, 'startStage')
        
        for row in rows:
            emapaCache[row[termCol].lower()] = row[keyCol]
            startStage[row[keyCol]] = row[stageCol]
            
    key = emapaCache['mouse']
    if structure:
        if structure.lower() in emapaCache:
            key = emapaCache[structure.lower()]
        if structure.lower().replace(' ', '') in emapaCache:
            key = emapaCache[structure.lower().replace(' ', '')]

    return key, startStage[key]
