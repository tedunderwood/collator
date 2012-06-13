''' Given a list of HathiTrust Volume IDs (HTids), retrieves those pagelists from
    a pairtree structure.

    Scans the pagelists to identify running headers. Organizes those headers
    into classes. Then uses the sequence of header codes to divide the volume
    into <div>...</div> segments, ignoring small ( < 4 pp.) segments.

    Saves (in the pairtree structure) a single text file that collates the
    pages, replacing plain-text apparatus for pagination with machine-readable
    <pb> and <div> tags. The <div> divisions are only approximate, of course.
    They are intended to guide segmentation for topic modeling, not as permanent
    contributions to the curation of these documents.
'''

import filekeeping
import os
from operator import itemgetter

pathdictionary = filekeeping.loadpathdictionary()

TabChar="\t"

dice_cutoff = .6

if "pairtreeroot" in pathdictionary:
    pairtree_rootpath = pathdictionary["pairtreeroot"]
else:
##   Hard-coding root path for development purposes, change this to your local root folder before running!
##    pairtree_rootpath = input("What is the path to the root folder for your pairtree structure? ")
    pairtree_rootpath = '/Users/mike/Desktop/collator/collection/'

# Placeholder. Eventually we can put some code here that gets a list of HTids to drive
# the process. For right now, I'm just choosing one arbitrarily as a test case.

HTids_toprocess = ['pst.000004048572']

# This is a special alphabet to be used in the bigram index.
alphabet = ['$', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k',
'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y',
'z']

def getbigrams(anystring):
    ''' Converts a string to a set of bigrams to be used for matching.'''

    global alphabet
    
    anystring = anystring.replace(' ', '$')
    anystring = '$' + anystring + '$'
    # In this index, $ is a special character that identifies the beginnings
    # and ends of words, so those can be represented in the bigram index.
     
    bigramdex = set()
    stringmax = len(anystring) - 1

    for idx, character in enumerate(anystring):
        if idx < stringmax:
            bigram = character + anystring[idx + 1]
            bigramdex.add(bigram)
        if idx > 0:
            bigram = anystring[idx - 1] + character
            bigramdex.add(bigram)

    return bigramdex

def dicecoefficient(firstset, secondset):
    '''Defines a similarity measure between two sets, in this case of bigrams.'''

    dice = (2 * len(firstset.intersection(secondset))) / (len(firstset) + len(secondset))
    return dice

for HTid in HTids_toprocess:

    # For each HTid, we get a path in the pairtree structure.
    # Then we read page files, and concatenate them in a list of pages
    # where each page is a list of lines.
    
    path, postfix = filekeeping.pairtreepath(HTid,pairtree_rootpath)
    pagepath = path + postfix + "/" + postfix + "/"
    pagefiles = os.listdir(pagepath)
    pagelist = []

    
    for f in pagefiles:
        if f[0] == ".":
            continue
        with open(pagepath + f, encoding='utf-8') as file:
            linelist = file.readlines()
            pagelist.append(linelist)

    # We're going to keep pageheaders rigorously aligned with pagelist,
    # so every page gets a 'header,' even if blank.
    
    pageheaders = []
    for page in pagelist:
        header = ""
        for line in page:
            # Current strategy: the running header is the first line
            # with more than four characters in it.
            
            if len(line) < 5 or line.isdigit():
                continue
            else:
                header = line.strip('1234567890.,"\t\n')
                # Here it would also be nice to have a function
                # that strips roman numerals, when they constitute
                # a separate word, without automatically stripping
                # all i's and v's from the header.
                
                break
            
        pageheaders.append(header)

    # Now we construct a dictionary where headers are associated with
    # the number of times they occur in pageheaders. Misspellings,
    # which occur frequently, will get their own entries. But we'll
    # deal with them in a moment. For right now we're just establishing
    # an order-in-which-to-consider the possibilities, which is going to
    # be based on frequency of occurrence.
    
    headerdict = {}
    for header in pageheaders:
        if header in headerdict:
            headerdict[header] += 1
        else:
            headerdict[header] = 1

    headersequence = sorted(headerdict.items(), key = itemgetter(1), reverse = True)

    # Reinitialize headerdict. It's now going to hold a dictionary of
    # translation rules mapping actually-occurring headers to normalized
    # header categories. Each header category is represented as a tuple
    # containing 0) the normalized header and 1) an integer code for it.
    #
    # We use a bigram-indexing strategy to figure out whether
    # each new header is "the same as" one already in the headerdict. If so, add a
    # translation rule to the headerdict. Otherwise, add it to the headerdict
    # as itself.
    #
    # valid_headers stores normalized header names, paired as tuples
    # with the bigram index for each so they can be checked as possible
    # matches.

    headerdict = {}
    valid_headers = []
    
    for header in headersequence:
        bigramdex = getbigrams(header[0])
        matched = False

        for idx, entrytuple in enumerate(valid_headers):
            possible_match, match_bigramdex  = entrytuple
            dice = dicecoefficient(bigramdex, match_bigramdex)
            
            if dice > dice_cutoff:
                headerdict[header[0]] = (possible_match, idx)
                matched = True

        if matched == False:
            entrytuple = header[0], bigramdex
            valid_headers.append(entrytuple)
            headerdict[header[0]] = (header[0], len(valid_headers))
            # We use the current length of valid_headers to
            # establish an integer code for this header category.

    # Now go back through the original list of pageheaders and use
    # headerdict to translate it into a list of header codes.
    
    headercodes = []
    for header in pageheaders:
        normalized, header_code = headerdict[header]
        headercodes.append(header_code)

    # Once the list of header codes has been established, run through the list
    # and count the number of pairings (both before and after)
    
    paircounts = {}
    
    for idx, code in enumerate(headercodes):
        
        if idx < len(headercodes) - 1:
            after = code, headercodes[idx + 1]
            if after in paircounts:
                paircounts[after] += 1
            elif (after[1],after[0]) in paircounts:
                paircounts[(after[1],after[0])] += 1
            else:
                paircounts[after] = 1

    ## First conditional in this set checks to make sure the before/after pairs
    ## don't match only if the loop isn't at the end of the header list
                
        if idx > 0:
            before = headercodes[idx - 1], code
            if idx < len(headercodes) -1 and before == (after[1],after[0]):  
                continue
            elif before in paircounts:
                paircounts[before] += 1
            elif (before[1],before[0]) in paircounts:
                paircounts[(before[1],before[0])] += 1
            else:
                paircounts[before] = 1
                
    validpairs = {}
    
    for pair in paircounts:
        if paircounts[pair] >= 4:
            validpairs[pair] = paircounts[pair]
            
    ## Go back through the list and assign section pairs numbers.  The dicionary
    ## sectiondict links header pairs (stored as tuples) to section codes.  The
    ## list will be the same length as pageheaders list and indicate which section
    ## each page belongs to.  Invalid sections assigned 999 as code for correction later.
    ## Checks before and after pairings, then resolves conflict by auto-assigning to
    ## more the common of the two.  The sectionlist allows for reverse look-up (for use
    ## in correction checks).
    
    sectioncodes = []
    sectiondict = {}
    sectionlist = []
    
    for idx, code in enumerate(headercodes):
        if idx < len(headercodes) - 1:
            s = code, headercodes[idx + 1]
            r = (s[1],s[0])
            if s in validpairs:
                if s not in sectiondict:
                    sectiondict[s] = len(sectiondict)
                    sectionlist.append(s)
                after = sectiondict[s]
            elif r in validpairs:
                if r not in sectiondict:
                    sectiondict[r] = len(sectiondict)
                    sectionlist.append(r)
                after = sectiondict[r]
            else:
                after = 999
        else:
            after = -1
                
    ## If not the first page, determine if the header pairing
    ## is an approved pairing.  
        if idx > 0:
            s = headercodes[idx - 1], code
            r = (s[1],s[0])
            if s in validpairs:
                if s not in sectiondict:
                    sectiondict[s] = len(sectiondict)
                    sectionlist.append[s]
                before = sectiondict[s]
            elif (r) in validpairs:
                if (r) not in sectiondict:
                    sectiondict[r] = len(sectiondict)
                    sectionlist.append(r)
                before = sectiondict[r]
            else:
                before = 999
        else:
            before = -1

    ## Determine which code to assign to the page by resolving any conflicts            
        if before == after:
            add = after
        elif before == -1 or before == 999:
            add = after
        elif after == -1 or after == 999:
            add = before
        elif paircounts[sectionlist[before]] > paircounts[sectionlist[after]]:
            add = before
        else:
            add = after
            
        sectioncodes.append(add)

    ## This loop examines the just created section code list to make corrections
    ## where invalid sections appear.  Start with 0 because first section should
    ## be zero.  First, check if beginning of index or end of index (since those
    ## are easy to fix).  Otherwise, count forward to for the next non-error code.
    ## Then compare distance between last known non-error and the next non-error.
    ## When correcting, update the last known section ID and last known index so
    ## that there's no ned to loop bakwards to make corrections.
    
    lastsection = 0
    lastknowndex = 0
    
    for idx,page in enumerate(sectioncodes):
        if page != 999:
            lastsection = page
            lastknowndex = idx
        elif idx == 0:
            for replace in sectioncodes:
                if replace != 999:
                    sectioncodes[idx] = replace
                    lastknowndex = idx
                    break
        elif idx == len(sectioncodes) - 1:
            sectioncodes[idx] = sectioncodes[idx - 1]
        else:
            count = 0
            for replace in sectioncodes[idx:]:
                count += 1
                if replace != 999:
                    break
            if (idx - lastknowndex) > count:
                sectioncodes[idx] = sectioncodes[idx + count]
                lastsection = page
                lastknowndex = idx
            elif (idx - lastknowndex) < count:
                sectioncodes[idx] = lastsection
                lastknowndex = idx
            elif paircounts[sectionlist[sectioncodes[idx - lastknowndex]]] > paircounts[sectionlist[sectioncounts[idx + count]]]:
                sectioncodes[idx] = lastsection
                lastknowndex = idx
            else:
                sectioncodes[idx] = sectioncodes[idx + count]
                lastsection = page
                lastknowndex = idx
                
    ## Step VI would start here now that sections are coded (or another correction
    ## loop for Step V 1/2 to remove isolated section IDs)
    
    for i,x in enumerate(sectioncodes):
        print(str(i + 1) + " " + str(x))
        
            