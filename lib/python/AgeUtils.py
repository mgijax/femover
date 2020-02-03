# Name: AgeUtils.py
# Purpose: handle various age transformations as we move data from back-end to front-end

import re

###--- globals ---###

embryonicRange = re.compile('^embryonic day *([0-9.]+) *- *([0-9.]+)$')
embryonicCommas = re.compile('^embryonic day *([0-9., ]+)$')

postnatalRange = re.compile('^postnatal ([a-z]+) *([0-9.]+) *- *([0-9.]+)$')
postnatalCommas = re.compile('^postnatal ([a-z]+) *([0-9., ]+)$')

# certain strings have a fixed range for ageMin/ageMax
standardTerms = {
    'postnatal' : 'P',
    'postnatal newborn' : 'P newborn',
    'postnatal adult' : 'P adult',
    }

# standard names for recognized timespans (that we will want to shorten to their first letter)
timespans = [ 'day', 'week', 'month', 'year' ]

###--- public functions ---###

def getAbbreviation(age):
    # convert the textual age to its abbreviation, if the format is recognized.
    # Returns: (string) abbreviation for the given 'age'
    
    if age == None:
        return None
    
    ageString = str(age).lower().strip()
    
    if ageString in standardTerms:
        return standardTerms[ageString]

    abbrev = _getEmbryonicAbbreviation(ageString)
    if abbrev != None:
        return abbrev
    
    return _getPostnatalAbbreviation(ageString)

###--- internal functions ---###

def _getEmbryonicAbbreviation(age):
    # handle the embryonic ages, returning appropriate abbreviation for 'age' if it is recognized

    match = embryonicRange.match(age)        # embryonic day x-y
    if match:
        try:
            return 'E%s-%s' % (match.group(1), match.group(2))
        except:
            pass                            # non-floats; fall through to None, None
    
    match = embryonicCommas.match(age)      # embryonic day x, y, z
    if match:
        try:
            return 'E%s' % match.group(1).replace(' ', '')
        except:
            pass                            # non-floats; fall through to None, None
    
    if age.startswith('embryonic'):
        return age.replace('embryonic', 'E')
    
    return None

def _getPostnatalAbbreviation(age):
    # handle the various 'postnatal' options

    match = postnatalRange.match(age)
    if match:
        (unit, rawMin, rawMax) = match.groups()
        if unit in timespans:
            try:
                if unit == 'day':
                    return 'P%s-%s' % (rawMin, rawMax)
                else:
                    return 'P %s %s-%s' % (unit[0], rawMin, rawMax)
            except:
                pass                        # non-floats; fall through
                
    match = postnatalCommas.match(age)
    if match:
        (unit, rawDays) = match.groups()
        if unit in timespans:
            try:
                if unit == 'day':
                    return 'P%s' % rawDays
                else:
                    return 'P %s %s' % (unit[0], rawDays)
            except:
                pass

    # last chance, see if any of the strings in 'standardTerms' is a prefix we can recognize (longest first)
    prefixes = standardTerms.keys()
    prefixes.sort(lambda x, y : -cmp(len(x), len(y)))
        
    for prefix in prefixes:
        if age.startswith(prefix):
            age2 = standardTerms[prefix]
            return age.replace(prefix, age2)

    return age
