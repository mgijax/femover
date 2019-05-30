# Name: RNASeqUtils.py
# Purpose: to provide utilities for gathering RNA-Seq expression data, so they can be
#   shared across multiple gatherers

import gc
import logger
import dbAgnostic
import FileReader

###--- public methods ---###

def getConsolidatedSamples():
    # returns list of rows for expression_ht_consolidated_sample table, where each is:
    #    [ consolidated_sample_key, experiment_key, genotype_key, organism, sex, age,
    #        age_min, age_max, emapa_key, theiler_stage, note, sequence_num ]

    return _getConsolidatedSamples()

def getSampleMap():
    # returns list of rows for expression_ht_sample_map table, where each is:
    #    [ unique_key, consolidated_sample_key, sample_key, sequence_num ]

    return _getSampleMap()

def getSampleMeasurements():
    # returns list of rows for expression_ht_sample_measurement table, where each is:
    #    [ measurement_key, sample_key, marker_key, marker_id, marker_symbol, average_tpm, qn_tpm ]
    
    return _getSampleMeasurements()

def getConsolidatedSampleMeasurements():
    # returns list of rows for expression_ht_consolidated_sample_measurment table, where each is:
    #    [ consolidated_measurement_key, consolidated_sample_key, marker_key, marker_id, marker_symbol,
    #        level, biological_replicate_count, average_qn_tpm ]
    
    return _getConsolidatedSampleMeasurements()

###--- private data and methods ---###

file1 = {
    'reader' : FileReader.FileReader('../data/E-ERAD-352_data.txt'),
    'experimentID' : 'E-ERAD-352',                                  # ID
    'markerColumn' : 0,                                             # column for marker key
    'sampleAvgTPM' : [ ('ERS626510', 1), ('ERS626511', 2) ],        # (ID, column) for sample's avg TPM
    'sampleQNTPM' : [ ('ERS626510', 5), ('ERS626511', 6) ],         # (ID, column) for sample's QN TPM
    'avgQNTPM' : 7,                                                 # column for marker's average QN TPM
    'level' : 8,                                                    # column for marker's expression level
    }
file2 = {
    'reader' : FileReader.FileReader('../data/E-GEOD-44366_data.txt'),
    'experimentID' : 'E-GEOD-44366',
    'markerColumn' : 0,
    'sampleAvgTPM' : [ ('GSM1083970 1', 1) ],
    'sampleQNTPM' : [ ('GSM1083970 1', 2) ],
    'avgQNTPM' : 3,
    'level' : 4,
    }
file3 = {
    'reader' : FileReader.FileReader('../data/E-MTAB-7279_data.txt'),
    'experimentID' : 'E-MTAB-7279',
    'markerColumn' : 0,
    'sampleAvgTPM' : [ ('Sample 31', 1), ('Sample 32', 2), ('Sample 33', 3), ('Sample 34', 4) ],
    'sampleQNTPM' : [ ('Sample 31', 5), ('Sample 32', 6), ('Sample 33', 7), ('Sample 34', 8) ],
    'avgQNTPM' : 9,
    'level' : 10,
    }

files = [ file1, file2, file3 ]

KEY_CONSOLIDATED_SAMPLE = 'consolidatedSampleKey'               # keys for 'maxKeys'
KEY_SAMPLE_MAP = 'sampleMapKey'
KEY_MEASUREMENT = 'measurementKey'
KEY_CONSOLIDATED_MEASUREMENT = 'consolidatedMeasurementKey'

maxKeys = {
    KEY_CONSOLIDATED_SAMPLE : 0,
    KEY_SAMPLE_MAP : 0,
    KEY_MEASUREMENT : 0,
    KEY_CONSOLIDATED_MEASUREMENT : 0,
}

def _nextKey(keyName):
    # get the next available value for the specified key name

    if keyName in maxKeys:
        maxKeys[keyName] = maxKeys[keyName] + 1
        return maxKeys[keyName]
    raise Exception('Unknown key type: %s' % keyName)

MGITYPE_EXPERIMENT = 42         # keys for 'objectKeys'
MGITYPE_SAMPLE = 43

objectKeys = {
    MGITYPE_EXPERIMENT : {},    # maps from experiment ID to experiment key
    MGITYPE_SAMPLE : {}         # maps from sample ID to sample key
    }

samples = {}                    # maps from sample name to sample key

def _getExperimentKey(accID):
    # look up and return the experiment key corresponding to the given 'accID'
    return _getObjectKey(accID, MGITYPE_EXPERIMENT)

def _getSampleKey(exptKey, sampleName):
    # look up and return the sample key corresponding to the given 'accID'
    if (exptKey, sampleName) in samples:
        return samples[(exptKey, sampleName)]

    cols, rows = dbAgnostic.execute('''select s._Sample_key
        from gxd_htsample s
        where s._Experiment_key = %d
            and s.name = '%s'
        limit 1''' % (exptKey, sampleName))
    
    if rows:
        samples[(exptKey, sampleName)] = rows[0][0]
        return rows[0][0]
    raise Exception('Unknown sample %s for %s' % (sampleName, exptID))

def _getObjectKey(accID, mgitypeKey):
    # look up and return the object key corresponding to the given accID / mgitypeKey pair
    
    if mgitypeKey not in objectKeys:
        raise Exception('Unknown MGIType key: %s' % mgitypeKey)
    
    if accID not in objectKeys[mgitypeKey]:
        cols, rows = dbAgnostic.execute('''select _Object_key
            from acc_accession
            where _MGIType_key = %d
                and accID = '%s'
            limit 1''' % (mgitypeKey, accID))
    
        if not rows:
            raise Exception('Unknown ID: %s' % accID)
        objectKeys[mgitypeKey][accID] = rows[0][0]

    return objectKeys[mgitypeKey][accID]

markerData = {}         # marker key : (symbol, ID)

def _cacheMarkers():
    if len(markerData) == 0:
        (cols, rows) = dbAgnostic.execute('''select m._Marker_key, m.symbol, a.accID
            from mrk_marker m, acc_accession a
            where m._Organism_key = 1
                and m._Marker_Status_key in (1,3)
                and m._Marker_key = a._Object_key
                and a._LogicalDB_key = 1
                and a.preferred = 1
                and a._MGIType_key = 2''')
        
        keyCol = dbAgnostic.columnNumber(cols, '_Marker_key')
        symbolCol = dbAgnostic.columnNumber(cols, 'symbol')
        idCol = dbAgnostic.columnNumber(cols, 'accID')
        
        for row in rows:
            markerData[row[keyCol]] = (row[symbolCol], row[idCol])
        logger.debug('Cached %d markers' % len(markerData))
    return

def _getMarkerID(markerKey):
    _cacheMarkers()
    mk = int(markerKey)
    if mk in markerData:
        return markerData[mk][1]
    return None

def _getMarkerSymbol(markerKey):
    _cacheMarkers()
    mk = int(markerKey)
    if mk in markerData:
        return markerData[mk][0]
    return None

INITIALIZED = False
CONSOLIDATED_SAMPLES = []
CONSOLIDATED_SAMPLE_MEASUREMENTS = []
SAMPLE_MEASUREMENTS = []
SAMPLE_MAP = []

def _buildFakeData():
    global INITIALIZED
    if not INITIALIZED:
        for file in files:
            _processFile(file)
            logger.debug('Processed %s' % file['reader'].getPath())
        
        INITIALIZED = True
    return

# maps from experiment key to [genotype key, organism, sex, age, age min, age max,
#    emapa key, theiler stage, note ]
details = {
    14038 : [ 12560, 'mouse laboratory', 'Male', 'E9.5', 9.5, 9.5, 18239165, 15, '21-somite stage embryo' ],
    5628 : [ 231, 'mouse laboratory', 'Not Specified', 'E13.5', 13.5, 13.5, 18242012, 22, None ],
    17319 : [ 17319, 'mouse laboratory', 'Female', 'P w 12-14', 105.01, 119.01, 18241303, 28, 'dorsal plus ventral thalamus' ],
    }

def _processFile(file):
    # load data from fake data file and populate global variables appropriately
    global CONSOLIDATED_SAMPLES, CONSOLIDATED_SAMPLE_MEASUREMENTS, SAMPLE_MAP, SAMPLE_MEASUREMENTS

    exptKey = _getExperimentKey(file['experimentID'])
    conSampleKey = _nextKey(KEY_CONSOLIDATED_SAMPLE)
    headers, rows = file['reader'].getData()
    rows = FileReader.nullify(rows)
    sampleIDs = map(lambda x: x[0], file['sampleAvgTPM'])

    # add consolidated samples (one per file)
    #    [ consolidated_sample_key, experiment_key, genotype_key, organism, sex, age,
    #        age_min, age_max, emapa_key, theiler_stage, note, sequence_num ]
    CONSOLIDATED_SAMPLES.append( [ conSampleKey, exptKey, ] + details[exptKey] + [ conSampleKey ] )
    
    # add map of consolidated samples to samples (one per sample in the file)
    #    [ unique_key, consolidated_sample_key, sample_key, sequence_num ]
    for sampleID in sampleIDs:
        SAMPLE_MAP.append([ _nextKey(KEY_SAMPLE_MAP), conSampleKey, _getSampleKey(exptKey, sampleID),
            len(SAMPLE_MAP) ])
    
    for row in rows:
        mrkKey = row[file['markerColumn']]

        # add sample measurements (one per marker/sample pair in the file)
        #    [ measurement_key, sample_key, marker_key, marker_id, marker_symbol, average_tpm, qn_tpm ]
    
        # caches from sample ID to QN TPM
        qnTpm = {}
        for (sampleID, colNum) in file['sampleQNTPM']:
            qnTpm[sampleID] = row[colNum]
        
        # generate one row per sample
        for (sampleID, colNum) in file['sampleAvgTPM']:
            SAMPLE_MEASUREMENTS.append([
                   _nextKey(KEY_MEASUREMENT), _getSampleKey(exptKey, sampleID),
                   mrkKey, _getMarkerID(mrkKey), _getMarkerSymbol(mrkKey), 
                   row[colNum], qnTpm[sampleID],
                   ])
            
        # add consolidated sample measurements (one per marker per file)
        #    [ consolidated_measurement_key, consolidated_sample_key, marker_key, marker_id, marker_symbol,
        #        level, biological_replicate_count, average_qn_tpm ]
        CONSOLIDATED_SAMPLE_MEASUREMENTS.append([
            _nextKey(KEY_CONSOLIDATED_MEASUREMENT), conSampleKey,
            mrkKey, _getMarkerID(mrkKey), _getMarkerSymbol(mrkKey), 
            row[file['level']], len(file['sampleQNTPM']), row[file['avgQNTPM']],
            ])
    
    return

def _getConsolidatedSamples():
    _buildFakeData()
    return CONSOLIDATED_SAMPLES

def _getSampleMap():
    _buildFakeData()
    return SAMPLE_MAP

def _getSampleMeasurements():
    _buildFakeData()
    return SAMPLE_MEASUREMENTS

def _getConsolidatedSampleMeasurements():
    _buildFakeData()
    return CONSOLIDATED_SAMPLE_MEASUREMENTS