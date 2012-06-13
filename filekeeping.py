'''
    Modules for managing pairtree files. We could expand this to include
    other io modules as appropriate.
'''

import glob

DictDirectory = "dictionaries/"

TabChar="\t"

def loadpathdictionary(path_to=""):
    '''Some of these scripts may eventually be exported for the use of
    other researchers grappling with HathiTrust data. Both for them, and
    in our own set-up, it's possible that data directories won't
    be in the same folder as the Python scripts themselves. So I'm going
    to store paths in a PathDictionary. Any path not in the dictionary,
    we'll have to query the user for -- and then store it in the dictionary.'''

    pathdictionary = {}
    
    if path_to == "":
        pathlist = glob.glob("PathDictionary.txt")
    else:
        pathlist = [path_to]
    
    for f in pathlist:
        with open(f, encoding='utf-8') as file:
            linelist = file.readlines()
        for workline in linelist:
            workline = workline.strip()
            words = workline.split(TabChar)
            pathdictionary[words[0]] = words[1]

    return pathdictionary

def pairtreepath(htid,rootpath):
    ''' Given a HathiTrust volume id, returns a relative path to that
    volume. I know that this function is too simple right now. It won't
    handle some of the funkier source id codes with internal slashes and
    colons. But it'll work for now; we'll update it later.
    While the postfix is part of the path, it's also useful to
    return it separately since it can be a folder/filename in its own
    right.'''
    
    period = htid.find('.')
    prefix = htid[0:period]
    postfix = htid[(period+1): ]
##    Setting path to mike's computer
##    path = '/Users/tunderwood/Hathi/' + prefix + '/pairtree_root/'
    path = rootpath + prefix + '/pairtree_root/'
    
    for i in range(0, 12, 2):
        next_two = postfix[i: (i+2)]
        path = path + next_two + '/'

    return path, postfix   
