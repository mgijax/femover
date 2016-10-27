# Name: AgeUtils.py
# Purpose: handle various age transformations as we move data from back-end to front-end

import re

embryonicAbbrev = re.compile('^e([0-9.]+)$')
embryonicSolo = re.compile('^e(mbryonic)? *(day)? *([0-9.]+)$')
embryonicPair = re.compile('^e(mbryonic)? *(day)? *([0-9.]+)-([0-9.]+)$')
embryonicTriple = re.compile('^e(mbryonic)? *(day)? *([0-9.]+), *([0-9.]+), *([0-9.]+)$')

postnatalAbbrev = re.compile('^p([0-9.]+)$')
postnatalSolo = re.compile('^p(ostnatal)? ([a-z]+) *([0-9.]+)$')
postnatalPair = re.compile('^p(ostnatal)? ([a-z]+) *([0-9.]+)-([0-9.]+)$')
postnatalTriple = re.compile('^p(ostnatal)? ([a-z]+) *([0-9.]+), *([0-9.]+), *([0-9.]+)$')

fixedRanges = {
    'embryonic' : ('E', 0.0, 21.0),
    'perinatal' : ('Perinatal', 17.0, 22.0),
    'postnatal' : ('P', 21.01, 1846.0),
    'postnatal newborn' : ('P newborn', 21.01, 25.0),
    'postnatal immature' : ('P immature', 25.01, 42.0),
    'postnatal adult' : ('P adult', 42.01, 1846.0),
    'p newborn' : ('P newborn', 21.01, 25.0),
    'p immature' : ('P immature', 25.01, 42.0),
    'p adult' : ('P adult', 42.01, 1846.0),
    }

multiplier = {
    'd' : 1,
    'day' : 1,
    'days' : 1,
    'w' : 7,
    'wk' : 7,
    'wks' : 7,
    'week' : 7,
    'weeks' : 7,
    'm' : 30,
    'month' : 30,
    'months' : 30,
    'y' : 365,
    'year' : 365,
    'years' : 365,
    }

def getAgeMinMax(age):
    # convert the textual age to its ageMin and ageMax equivalents, if the format is recognized
    # Returns: (ageMin, ageMax) or (None, None) if age cannot be converted
    
    if age == None:
        return None, None, None
    
    ageString = str(age).lower().strip()
    ageString = tweakAge(ageString)           # TODO: remove once we have curated samples
    
    if ageString in fixedRanges:
        return fixedRanges[ageString]

    abbrev, ageMin, ageMax = _getEmbryonicMinMax(ageString)
    if ageMin:
        return abbrev, ageMin, ageMax
    
    abbrev, ageMin, ageMax = _getPostnatalMinMax(ageString)
    if ageMin:
        return abbrev, ageMin, ageMax

    return age, None, None

def tweakAge(age):
    # tries to intelligently adapt a non-standard age format to one of our standard age formats
    # TODO: remove this once we start having curated samples
    
    ageString = age.lower().strip().replace(' to ', '-').replace('post natal', 'postnatal').replace(' old', '').replace(' since birth', '')

    solo = re.compile('^([0-9.]+) ([a-z]+)s$')    # 123 units (postnatal implied)
    match = solo.match(ageString)
    if match:
        rawAge, unit = match.groups()
        if unit in multiplier:
            try:
                ageMin = float(rawAge)
                return 'postnatal %s %s' % (unit, rawAge)
            except:
                pass
    
    pair = re.compile('^([0-9.]+)-([0-9.]+) ([a-z]+)s$')      # 123-456 units (postnatal implied)
    match = pair.match(ageString)
    if match:
        rawMin, rawMax, unit = match.groups()
        if unit in multiplier:
            try:
                ages = map(float, [ rawMin, rawMax ])
                return 'postnatal %s %s-%s' % (unit, rawMin, rawMax)
            except:
                pass
            
    return age
    
def _getEmbryonicMinMax(age):
    # handle the embryonic ages, returning min/max for 'age' if it is recognized

    match = embryonicAbbrev.match(age)        # ex
    if match:
        try:
            fAge = float(match.group(1))
            return 'E%s' % match.group(1), fAge, fAge
        except:
            pass

    match = embryonicSolo.match(age)        # embryonic day x
    if match:
        try:
            fAge = float(match.group(-1))
            return 'E%s' % match.group(-1), fAge, fAge
        except:
            pass

    match = embryonicPair.match(age)        # embryonic day x-y
    if match:
        try:
            pair = map(float, match.groups()[-2:])
            return 'E%s-%s' % (pair[0], pair[1]), min(pair), max(pair)
        except:
            pass                            # non-floats; fall through to None, None
    
    match = embryonicTriple.match(age)      # embryonic day x, y, z
    if match:
        try:
            triple = map(float, match.groups()[-3:])
            return 'E%s,%s,%s' % (triple[0], triple[1], triple[2]), min(triple), max(triple)
        except:
            pass                            # non-floats; fall through to None, None
    
    return age, None, None

def _getPostnatalMinMax(age):
    # handle the various 'postnatal' options

    fMin = None         # float rawMin (before multiplier)
    fMax = None         # float rawMax (before multiplier)
    days = None         # number of days for age unit
    
    abbrev = age

    match = postnatalAbbrev.match(age)        # px
    if match:
        try:
            fMin = float(match.group(1))
            fMax = fMin
            days = 1
        except:
            pass

    if (days == None) or (fMin == None) or (fMax == None):
        match = postnatalSolo.match(age)
        if match:
            (skip, unit, rawAge) = match.groups()
            if unit in multiplier:
                days = multiplier[unit]
                try:
                    fMin = float(rawAge)
                    fMax = fMin
                    if unit == 'day':
                        abbrev = 'P%s' % rawAge
                    else:
                        abbrev = 'P %s%s' % (unit[0], rawAge)
                except:
                    pass

    if (days == None) or (fMin == None) or (fMax == None):
        match = postnatalPair.match(age)
        if match:
            (skip, unit, rawMin, rawMax) = match.groups()
            if unit in multiplier:
                days = multiplier[unit]
                try:
                    pair = map(float, [ rawMin, rawMax ] )
                    fMin = min(pair)
                    fMax = max(pair)
                    
                    if unit == 'day':
                        abbrev = 'P%s-%s' % (rawMin, rawMax)
                    else:
                        abbrev = 'P %s%s-%s' % (unit[0], rawMin, rawMax)
                except:
                    pass                        # non-floats; fall through to None, None
                
    if (days == None) or (fMin == None) or (fMax == None):
        match = postnatalTriple.match(age)
        if match:
            (skip, unit, x, y, z) = match.groups()
            if unit in multiplier:
                days = multiplier[unit]
                try:
                    triple = map(float, [ x, y, z ])
                    fMin = min(triple)
                    fMax = max(triple)
                    
                    if unit == 'day':
                        abbrev = 'P%s,%s,%s' % (x, y, z)
                    else:
                        abbrev = 'P %s%s,%s,%s' % (unit[0], x, y, z)
                except:
                    pass

    if (fMin != None) and (fMax != None) and (days != None):
        return abbrev, (days * fMin + 21.01), (days * fMax + 21.01)
        
    return age, None, None
