# Module: ReferenceCitations.py
# Purpose: to provide an easy means to access various versions of reference
#       citations, and to provide for their creation in one place

import dbAgnostic
import logger

###--- Globals ---###

# database table name; if not None, must contain a _Refs_key field, which is
# used to restrict the set of references we retrieve.  set by restrict().
SOURCE_TABLE = None

MINI = {}       # reference key -> mini citation
SHORT = {}      # reference key -> short citation
LONG = {}       # reference key -> long citation

SEQUENCE_NUM = {}       # reference key -> integer to sort by long citation
MINI_SEQ_NUM = {}       # reference key -> integer to sort by mini citation

###--- Private Functions ---###

def _getBooks():
        # cache book attributes

        bookCmd = '''select _Refs_key,
                        book_au as editors,
                        book_title,
                        place,
                        publisher,
                        series_ed as edition
                from BIB_Books'''

        if SOURCE_TABLE:
                bookCmd = bookCmd + ''' b
                        where exists (select 1 from %s c
                                where b._Refs_key = c._Refs_key)''' % SOURCE_TABLE

        books = {}

        columns, results = dbAgnostic.execute (bookCmd)

        keyCol = dbAgnostic.columnNumber (columns, '_Refs_key')
        editorsCol = dbAgnostic.columnNumber (columns, 'editors')
        titleCol = dbAgnostic.columnNumber (columns, 'book_title')
        placeCol = dbAgnostic.columnNumber (columns, 'place')
        publisherCol = dbAgnostic.columnNumber (columns, 'publisher')
        editionCol = dbAgnostic.columnNumber (columns, 'edition')

        for r in results:
                key = r[keyCol]
                book = {
                        'editors' : r[editorsCol],
                        'bookTitle' : r[titleCol],
                        'place' : r[placeCol],
                        'publisher' : r[publisherCol],
                        'edition' : r[editionCol],
                        }
                books[key] = book
        
        logger.debug ('Got data for %d books' % len(books))

        return books

def _getRefs():
        # cache other needed reference data 

        refs = {}

        allCmd = '''select r._Refs_key,
                        c.referenceType,
                        r.authors,
                        r.title,
                        r.journal,
                        r.vol,
                        r.issue,
                        r.date as pubDate,
                        r.year,
                        r.pgs as pages
                from BIB_Refs r, BIB_Citation_Cache c
                where r._Refs_key = c._Refs_key'''

        if SOURCE_TABLE:
                allCmd = allCmd + '''
                        and exists (select 1 from %s d
                                where d._Refs_key = r._Refs_key)''' % SOURCE_TABLE

        columns, results = dbAgnostic.execute (allCmd)

        keyCol = dbAgnostic.columnNumber (columns, '_Refs_key')
        typeCol = dbAgnostic.columnNumber (columns, 'referenceType')
        authorCol = dbAgnostic.columnNumber (columns, 'authors')
        titleCol = dbAgnostic.columnNumber (columns, 'title')
        journalCol = dbAgnostic.columnNumber (columns, 'journal')
        volCol = dbAgnostic.columnNumber (columns, 'vol')
        issueCol = dbAgnostic.columnNumber (columns, 'issue')
        dateCol = dbAgnostic.columnNumber (columns, 'pubDate')
        yearCol = dbAgnostic.columnNumber (columns, 'year')
        pagesCol = dbAgnostic.columnNumber (columns, 'pages')

        for row in results:
                out = {
                        '_Refs_key' : row[keyCol],
                        'referenceType' : row[typeCol],
                        'journal' : row[journalCol],
                        'vol' : row[volCol],
                        'issue' : row[issueCol],
                        'pubDate' : row[dateCol],
                        'year' : row[yearCol],
                        'pages' : row[pagesCol],
                        'authors' : row[authorCol],
                        'title' : row[titleCol],
                        }

                refs[row[keyCol]] = out

        logger.debug ('Got data for %d refs' % len(refs))

        return refs

def _getAuthors (longAuthors):
        # if multiple authors, show only the first for mini and short
        # citations (with trailing comma, space)

        miniCitation = ''
        shortCitation = ''
        longCitation = ''

        if longAuthors:
                semiPos = longAuthors.find(';')
                if semiPos >= 0:
                        authors = longAuthors.split(';')[0].strip()
                        miniCitation = authors + ', et al., '
                else:
                        miniCitation = longAuthors + ', '

                shortCitation = miniCitation 
                longCitation = longAuthors.replace (';', ',') + ', '

        return miniCitation, shortCitation, longCitation


def _getBookCitations (refsKey, refs, books):

        # three different citation types with varying author formats

        miniCitation, shortCitation, longCitation = _getAuthors (
                refs[refsKey]['authors'])

        # get book data

        editors = None
        place = None
        bookTitle = None
        publisher = None
        edition = None

        if refsKey in books:
                editors = books[refsKey]['editors']
                place = books[refsKey]['place']
                bookTitle = books[refsKey]['bookTitle']
                publisher = books[refsKey]['publisher']
                edition = books[refsKey]['edition']

        # chapter title

        if refs[refsKey]['title']:
                shortCitation = shortCitation + refs[refsKey]['title']
                longCitation = longCitation + refs[refsKey]['title']
                if bookTitle:
                        shortCitation = shortCitation + ', '
                        longCitation = longCitation + ', '
                else:
                        shortCitation = shortCitation + '. '
                        longCitation = longCitation + '. '

        # book editors 
        
        titleIn = ' in '
        if editors:
                editors = editors.replace (';', ',')
                longCitation = longCitation + ' in ' + editors + ' (eds), '
                titleIn = ''

        # book title 

        if bookTitle:
                miniCitation = miniCitation + ' in ' + bookTitle + '. '
                shortCitation = shortCitation + ' in ' + bookTitle + '. '
                longCitation = longCitation + titleIn + bookTitle + '. '

        # edition

        if edition:
                longCitation = longCitation + edition + '. '

        # date

        date = refs[refsKey]['pubDate']         # date preferred
        if not date:
                date = refs[refsKey]['year']    # settle for year

        if date:
                miniCitation = miniCitation + date
                shortCitation = shortCitation + date
                longCitation = longCitation + date

        # pages

        pages = refs[refsKey]['pages']
        if pages:
                miniCitation = miniCitation + ':' + pages + '.'
                shortCitation = shortCitation + ':' + pages + '.'
                longCitation = longCitation + ':' + pages + '. '

        # place

        if place:
                longCitation = longCitation + place + ': '

        # publisher

        if publisher:
                longCitation = longCitation + publisher + '.'

        return miniCitation, shortCitation, longCitation

def _getArticleCitations (refsKey, refs, books):

        # three different citation types with varying formats

        miniCitation, shortCitation, longCitation = _getAuthors (
                refs[refsKey]['authors'])

        # for short and long, include the title (with period, space)

        longTitle = refs[refsKey]['title']
        if longTitle:
                # add a final period to the title if it's missing
                if not longTitle.endswith('.'):
                        longTitle = longTitle + '.'

                # add a space
                title = longTitle + ' '

                shortCitation = shortCitation + title
                longCitation = longCitation + title

        # From here on, all three formats are the same.  We will compose this
        # tail portion together in one string and then append it to the
        # individual citations when complete.

        tail = ''

        # journal (abbreviation, period, space)
        # TO DO: switch from journal to its abbreviation

        journalAbbrev = refs[refsKey]['journal']
        if journalAbbrev:
                tail = tail + journalAbbrev + '. '

        # year (or date, if it exists), then semicolon

        date = refs[refsKey]['pubDate']         # date preferred
        if not date:
                date = str(refs[refsKey]['year'])       # settle for year

        if date:
                tail = tail + date + ';'

        # volume

        if refs[refsKey]['vol']:
                tail = tail + refs[refsKey]['vol']

        # issue in parentheses

        if refs[refsKey]['issue']:
                tail = tail + '(' + refs[refsKey]['issue'] + ')'

        # pages preceded by colon

        if refs[refsKey]['pages']:
                tail = tail + ':' + refs[refsKey]['pages']

        # finish three citations and return them

        miniCitation = miniCitation + tail
        shortCitation = shortCitation + tail
        longCitation = longCitation + tail

        return miniCitation, shortCitation, longCitation

def _initialize():
        global MINI, SHORT, LONG, SEQUENCE_NUM, MINI_SEQ_NUM

        MINI = {}
        SHORT = {}
        LONG = {}

        books = _getBooks()
        refs = _getRefs()

        toOrder = []
        toOrderMini = []

        referenceKeys = list(refs.keys())

        for key in referenceKeys:
                if refs[key]['referenceType'].upper() == 'BOOK':
                        miniCitation, shortCitation, longCitation = \
                                _getBookCitations (key, refs, books)
                else:
                        miniCitation, shortCitation, longCitation = \
                                _getArticleCitations (key, refs, books)

                MINI[key] = miniCitation
                SHORT[key] = shortCitation
                LONG[key] = longCitation

                toOrder.append ( (longCitation.lower(), key) )
                toOrderMini.append ( (miniCitation.lower(), key) )

        logger.debug ('Built citations for %d refs' % len(MINI))

        toOrder.sort()
        toOrderMini.sort()

        i = 0
        for (citation, key) in toOrder:
                i = i + 1
                SEQUENCE_NUM[key] = i

        j = 0
        for (citation, key) in toOrderMini:
                j = j + 1
                MINI_SEQ_NUM[key] = j

        logger.debug ('Ordered %d refs by citation' % i)
        return

###--- Functions ---###

def getMiniCitation (refsKey):
        if not MINI:
                _initialize()

        if refsKey in MINI:
                return MINI[refsKey]
        return None

def getShortCitation (refsKey):
        if not SHORT:
                _initialize()

        if refsKey in SHORT:
                return SHORT[refsKey]
        return None

def getLongCitation (refsKey):
        if not LONG:
                _initialize()

        if refsKey in LONG:
                return LONG[refsKey]
        return None

def getSequenceNum (refsKey):
        if not SEQUENCE_NUM:
                _initialize()

        if refsKey in SEQUENCE_NUM:
                return SEQUENCE_NUM[refsKey]
        return len(SEQUENCE_NUM) + 1

def getSequenceNumByMini (refsKey):
        if not MINI_SEQ_NUM:
                _initialize()

        if refsKey in MINI_SEQ_NUM:
                return MINI_SEQ_NUM[refsKey]
        return len(MINI_SEQ_NUM) + 1

def restrict(tableName):
        # if you'd like to restrict reference collection to only those cited
        # in a certain table, call this first and specify the table name
        # (to save memory and time)

        global SOURCE_TABLE

        SOURCE_TABLE = tableName
        logger.debug('Restricted set of references to only those in %s' % SOURCE_TABLE)
        return

