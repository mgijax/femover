# Name: AgeUtils.py
# Purpose: handle various age transformations as we move data from back-end to front-end

import re

###--- globals ---###

embryonicRange = re.compile('^embryonic day *([0-9.]+) *- *([0-9.]+)$')
embryonicCommas = re.compile('^embryonic day *([0-9., ]+)$')

postnatalRange = re.compile('^postnatal ([a-z]+) *([0-9.]+) *- *([0-9.]+)$')
postnatalCommas = re.compile('^postnatal ([a-z]+) *([0-9., ]+)$')

# certain strings have a fixed range for ageMin/ageMax
fixedRanges = {
    'postnatal' : ('P', 21.01, 1846.0),
    'postnatal newborn' : ('P newborn', 21.01, 25.0),
    'postnatal adult' : ('P adult', 42.01, 1846.0),
    }

# number of days per postnatal unit, to use in computation of ageMin / ageMax
multiplier = {
    'day' : 1,
    'week' : 7,
    'month' : 30,
    'year' : 365,
    }

###--- public functions ---###

def getAgeMinMax(age):
    # convert the textual age to its abbreviation and its ageMin / ageMax equivalents, if the format is recognized.
    # Returns: (abbreviation, ageMin, ageMax) or (None, None, None) if age cannot be converted
    
    if age == None:
        return None, None, None
    
    ageString = str(age).lower().strip()
    
    if ageString in fixedRanges:
        return fixedRanges[ageString]

    abbrev, ageMin, ageMax = _getEmbryonicMinMax(ageString)
    if ageMin != None:
        return abbrev, ageMin, ageMax
    
    abbrev, ageMin, ageMax = _getPostnatalMinMax(ageString)
    if ageMin != None:
        return abbrev, ageMin, ageMax

    return age, None, None

###--- internal functions ---###

def _roundAge(floatDay):
    # rounds 'floatDay' such that we have 3-way rounding:
    # 1. if the fractional part is under a quarter, round down to the integer part
    # 2. if the fractional part is between a quarter and three-quarters, round to the half
    # 3. if the fractional part is more than three-quarters, round up to the next integer part
    
    intPart = int(floatDay)
    fraction = floatDay - intPart

    if fraction < 0.25:
        return intPart * 1.0
    elif fraction < 0.75:
        return intPart + 0.5
    return intPart + 1.0

def _convertDaysToFloats(dayString):
    # convert from a string of comma-separated days to a list of floats, or return an empty list if errors occur
    if not dayString:
        return []
    
    try:
        return map(lambda f: float(f), map(lambda x: x.strip(), dayString.split(',')))
    except:
        return []

def _minAge(dayString):
    # Get the minimum age included in the given comma-separated list of days, or None if errors occur.
    # Note that this is rounded to the nearest half.
    
    days = _convertDaysToFloats(dayString)
    if not days:
        return None
    return min(map(lambda x: _roundAge(x), days))

def _maxAge(dayString):
    # Get the maximum age included in the given comma-separated list of days, or None if errors occur.
    # Note that this is rounded to the nearest half.
    
    days = _convertDaysToFloats(dayString)
    if not days:
        return None
    return max(map(lambda x: _roundAge(x), days))

def _getEmbryonicMinMax(age):
    # handle the embryonic ages, returning min/max for 'age' if it is recognized

    match = embryonicRange.match(age)        # embryonic day x-y
    if match:
        try:
            pair = '%s,%s' % (match.group(1), match.group(2))
            return 'E%s-%s' % (match.group(1), match.group(2)), _minAge(pair), _maxAge(pair)
        except:
            pass                            # non-floats; fall through to None, None
    
    match = embryonicCommas.match(age)      # embryonic day x, y, z
    if match:
        try:
            dayString = match.group(1).replace(' ', '')
            return 'E%s' % dayString, _minAge(match.group(1)), _maxAge(match.group(1))
        except:
            pass                            # non-floats; fall through to None, None
    
    if age.startswith('embryonic'):
        return age.replace('embryonic', 'E'), 0.0, 21.0
    
    return age, None, None

def _getPostnatalMinMax(age):
    # handle the various 'postnatal' options

    fMin = None         # float rawMin (before multiplier)
    fMax = None         # float rawMax (before multiplier)
    days = None         # number of days for age unit
    
    abbrev = age

    match = postnatalRange.match(age)
    if match:
        (unit, rawMin, rawMax) = match.groups()
        if unit in multiplier:
            days = multiplier[unit]
            try:
                fMin = _minAge('%s,%s' % (rawMin, rawMax))
                fMax = _maxAge('%s,%s' % (rawMin, rawMax))
                    
                if unit == 'day':
                    abbrev = 'P%s-%s' % (rawMin, rawMax)
                else:
                    abbrev = 'P %s %s-%s' % (unit[0], rawMin, rawMax)
            except:
                pass                        # non-floats; fall through to None, None
                
    if (days == None) or (fMin == None) or (fMax == None):
        match = postnatalCommas.match(age)
        if match:
            (unit, rawDays) = match.groups()
            if unit in multiplier:
                days = multiplier[unit]
                try:
                    fMin = _minAge(rawDays)
                    fMax = _maxAge(rawDays)
                    
                    if unit == 'day':
                        abbrev = 'P%s' % rawDays
                    else:
                        abbrev = 'P %s %s' % (unit[0], rawDays)
                except:
                    pass

    # last chance, see if any of the strings in 'fixedRanges' is a prefix we can recognize (longest first)
    if (days == None) or (fMin == None) or (fMax == None):
        prefixes = fixedRanges.keys()
        prefixes.sort(lambda x, y : -cmp(len(x), len(y)))
        
        for prefix in prefixes:
            if age.startswith(prefix):
                age2, ageMin, ageMax = fixedRanges[prefix]
                return age.replace(prefix, age2), ageMin, ageMax

    if (fMin != None) and (fMax != None) and (days != None):
        return abbrev, (days * fMin + 21.01), (days * fMax + 21.01)
        
    return age, None, None
