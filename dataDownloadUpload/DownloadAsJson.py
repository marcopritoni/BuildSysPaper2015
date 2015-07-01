from smap.archiver.client import SmapClient # Used for querying database.
import csv
import ConfigParser # Used to collect information from the config file.
import copy # Used to deepcopy dictionaries
import json # Used to output JSON file.
import sys # Needed for command-line arguments and early-exits
import collections # Used to create ordereddict


# Note: Collection class unrelated to 'collections' module.
class Collection:
    # self.subCollections
    #   All child-collections of this one.
    #
    # self.streamRefs
    #   Contains references to all collections in the hierarchy that
    #   are streams.
    #
    # self.tags
    #   Contains all tagname/value pairs that belong to this collection
    #
    # self.isStream
    #   True if this is a stream at the bottom of the hierarchy.
    #   False otherwise.
    #
    # self.path
    #   The path to this given collection.
    #
    # self.refList
    #   A list of references to all Collections in the hierarchy. Only belongs
    #   to master Collection.
    #
    # self.isMaster
    #   True if this is the master Collection.
    #   False otherwise.

    
    def __init__(self, streams=None):
        self.subCollections = {}
        self.streamRefs = []
        self.tags = {}
        self.isStream = False
        self.path = '/' # The master collection's path is a '/'.
        self.refList = []
        # If a list of stream dicts is passed, construct hierarchy, with
        # this Collection as master.
        if not streams is None:
            self.master = True
            self._constructFromList(streams)
        # END: if not streams is None
        # If no argument is given, this is not a master.
        else:
            self.master = False
        # END: else
    # END: def __init__(self, streams=None)


    # Constructs collection hierarchy from list of streams dicts.
    def _constructFromList(self, streams):
        self.subCollections = {}
        allColls = []

        # Construct overall hierarchy from list of streams
        for stream in streams:
            collRef = self
            splitPath = stream['Path'].split('/')
            if not splitPath[0]:
                splitPath = splitPath[1:]
            # END: if not splitPath[0]

            pathSoFar = ''
            for cName in splitPath:
                pathSoFar = pathSoFar + '/' + cName
                if not cName in collRef.subCollections:
                    collRef.subCollections[cName] = Collection()
                    collRef.subCollections[cName].path = pathSoFar
                    allColls.append(collRef.subCollections[cName])
                # END: if not cName in collRef.subCollections
                collRef = collRef.subCollections[cName]
            # END: for cName in splitPath
            collRef.isStream = True
            collRef.tags = stream
        # END: for stream in streams

        self.refList = [self] + allColls
        self._recursiveStreamRefs()
        # Move redundant tags to parent collections
        self._graduateTags()
    # END: def _constructFromList(self, streams)

    # Gets references to all streams below this Collection, and stores them in
    # list of streamRefs.
    def _recursiveStreamRefs(self):
        for c in self.subCollections:
            if self.subCollections[c].isStream:
                self.streamRefs.append(c)
            # END: if self.subCollections[c].isStream
            else:
                self.subCollections[c]._recursiveStreamRefs()
                self.streamRefs += self.subCollections[c].streamRefs
            # END: else
    # END: def _recursiveStreamRefs(self)


    # Convert all tags to flat form (TagPortion1/TagPortion2/etc...)
    # to simplify comparison, and moves them up the hierarchy
    def _graduateFlatTags(self):
        candidates = {}
        popList = []
        exceptions = ['uuid','Path','Properties']
        firstIteration = True

        for coll in self.subCollections:
            popList = []
            flatTags = None
            self.subCollections[coll]._graduateFlatTags()
            if self.subCollections[coll].isStream:
                flatTags = makeFlatTagDict(self.subCollections[coll].tags)
                self.subCollections[coll].tags = flatTags
            # END: if self.subCollections[coll].isStream
            else:
                flatTags = self.subCollections[coll].tags
            # END: else
            if firstIteration:
                firstIteration = False
                candidates = copy.deepcopy(flatTags)
                for val in exceptions:
                    for tag in candidates:
                        if val == tag.split('/')[0]:
                            popList.append(tag)
                        # END: if val == tag.split('/')[0]
                    # END: for tag in candidates
                # END: for val in exceptions
                for tag in popList:
                    candidates.pop(tag, None)
                # END: for tag in popList
            # END: if firstIteration
            else:
                for tag in candidates:
                    if not tag in flatTags or flatTags[tag] != candidates[tag]:
                        popList.append(tag)
                    # END: if not tag in flatTags or flatTags[tag] !=
                    #      candidates[tag]
                # END: for tag in candidates
                for tag in popList:
                    candidates.pop(tag, None)
                # END: for tag in popList
                popList = []
            # END: else
        # END: for coll in self.subCollections
                
        for coll in self.subCollections:
            for tag in candidates:
                if tag in self.subCollections[coll].tags:
                    self.subCollections[coll].tags.pop(tag, None)
                # END: if tag in self.subCollections[coll].tags
            # END: for tag in candidates
        # END: for coll in self.subCollections
        
        if not self.isStream:
            self.tags = candidates
        # END: if not self.isStream
    # END: def _graduateFlatTags(self)


    def _deepenAllTags(self):
        self.tags = deepenTagDict(self.tags)
        for coll in self.subCollections:
            self.subCollections[coll]._deepenAllTags()
        # END: for coll in self.subCollections
    # END: def _deepenAllTags(self)


    # For each tag, if it is identical across all subcollections of a
    # collection, remove the tag from the subcollections, and apply it to the
    # collection containing them.
    def _graduateTags(self):
        self._graduateFlatTags()
        self._deepenAllTags()
    # END: def _graduateTags(self)


    # Assigns readings to this stream
    def assignReadings(self, start='4/1/2015', end='4/30/2015',
                       beforeNow=True, limit=100):
        global c
        if not self.isStream:
            print "Error: getReadings called from non-stream. Exiting."
            sys.exit()
        # END: if not self.isStream

        if beforeNow:
            timeRangeClause = 'before now'
        # END: if beforeNow
        else:
            timeRangeClause = 'in ("' + start + '", "' + end + '")'
        # END: else

    
        q = c.query('select data ' + timeRangeClause + ' limit ' +
                          str(limit) + ' where uuid = "' +
                          self.tags['uuid'] + '"')

        for elem in q[0]['Readings']:
            elem[0] = int(elem[0])
        # END: for elem in q[0]['Readings']
        
        self.tags['Readings'] = q[0]['Readings']
    # END: def assignReadings(self, start, end, beforeNow, limit)


    # Assigns readings to every stream below this Collection.
    def assignReadingsToAll(self, strt='4/1/2015', e='4/30/2015',
                            bfrNow=True, lmt=100):
        if self.isStream:
            self.assignReadings(strt, e, bfrNow, lmt)
        # END: if self.isStream
        for coll in self.subCollections:
            self.subCollections[coll].assignReadingsToAll(strt, e, bfrNow, lmt)
        # END: for coll in self.subCollections
    # END: def assignReadings(self, strt, e, bfrNow, lmt)


    # Currently unused. removes readings if this is a stream. Might be useful
    # if data is substantially large, so one doesn't need to have all readings
    # loaded in at once.
    def removeReadings(self):
        if not self.isStream:
            print "Error: removeReadings called from non-stream. Exiting."
            sys.exit()
        # END: if not self.isStream

        self.tags.pop('Readings', None)
    # END: def removeReadings(self)


    # Not used.
    def asShallowDict(self):
        retDict = {}
        retDict[self.path] = {}
        objDict = retDict[self.path]
        if not self.isStream:
            objDict['Contents'] = list(self.subCollections.keys())
        for tag in self.tags:
            objDict[tag] = self.tags[tag]

        return retDict
    # END: def asShallowDict(self)


    # Converts entire hierarchy to a dictionary structure, ready for output
    # to JSON
    def asFullDict(self):
        retDict = collections.OrderedDict()
        if not self.master:
            print "Error: fullDict must be called from master Collection" \
                  "object. Exiting."
            sys.exit()

        for coll in self.refList:
            retDict[coll.path] = {}
            curDict = retDict[coll.path]
            if not coll.isStream:
                curDict['Contents'] = list(coll.subCollections.keys())
            for tag in coll.tags:
                curDict[tag] = coll.tags[tag]

        return retDict
    # END: def asFullDict(self)
# END: class Collection
        

# Outputs dictionary d to a json file of the name outputFile.
def dictToJson(d, outputFile):
    with open(outputFile, 'w') as f:
        json.dump(d, f, indent=5)
    f.close()
# END: def dictToJson(d)


# Converts nested tags (eg. {'Metadata':{Extra:{PointName:Blahblah}}})
# in a dictionary to flat, pathlist-style tags
# (eg. {Metadata/Extra/PointName:Blahblah})
def makeFlatTagDict(d):
    newDict = {}
    for key in d:
        if type(d[key]) is dict:
            curDict = makeFlatTagDict(d[key])
            for subKey in curDict:
                newDict[key + '/' + subKey] = curDict[subKey]
        else:
            newDict[key] = d[key]

    return newDict
# END: def makeFlatTagDict(d)


    #for cName in splitPath:
    #    if not cName in collRef.subCollections:
    #                collRef[cName] = Collection()
    #    collRef = collRef[cName]
    #collRef.isStream = True
    #collRef.tags = copy.deepcopy(stream)


def deepenTagDict(d):
    newDict = {}
    lastPart = None
    for key in d:
        prevRef = newDict
        newRef = newDict
        splitKey = key.split('/')
        for part in splitKey:
            if not part in newRef:
                newRef[part] = {}
            prevRef = newRef
            newRef = newRef[part]
            lastPart = part
            
        prevRef[lastPart] = d[key]

    return newDict
# END: def deepenTagDict(d)


# Gets server address, where-clause, and output file name from config file.
def getConfigInfo(fName):
    cnfg = ConfigParser.ConfigParser()
    cnfg.read(fName)
    serverName = cnfg.get('sysID', 'client')
    whereClause = cnfg.get('sysID','where')
    outputFileName = cnfg.get('sysID', 'output')

    return serverName, whereClause, outputFileName
# END: def getConfigInfo(fName)


def readCommandLine():
    if len(sys.argv) != 2:
        print "Improper arguments used. Format:"
        print "python " + sys.argv[0] + " <config file name>"
        sys.exit()

    return sys.argv[1]
# END: def readCommandLine


# Queries database with config-specified where-clause w.
# Returns list of stream metadata dicts.
def getSeriesList(w):
    global c
    qList = c.query('select * where ' + w)

    return qList
# END: def getSeriesList(w)


#def makeDictHierarchy(seriesList):
#    h = {}
#    streamRefs = []

#    # Generate hierarchy
#    for series in seriesList:
#        collRef = h
#        splitPath = series['Path'].split('/')
#        if not splitPath[0]:
#            splitPath = splitPath[1:]
#        for cName in splitPath:
#            if not cName in collRef:
#                collRef[cName] = {}

#            collRef = collRef[cName]
#        streamRefs.append(collRef)

    
#    return h, streamRefs


# Debug Function
def testFlattenDeepen():
    x = SmapClient("http://www.openbms.org/backend")
    testDict = x.query('select * where uuid = '
                       '"810e0b67-b6ea-5407-827d-3691b7a2f2f1"')
    testDict = testDict[0]
    print "initial:"
    for elem in testDict:
        print elem + ": " + str(testDict[elem])
    print "\nPaths:"
    pathsDict = makeFlatTagDict(testDict)
    for elem in pathsDict:
        print elem + ": " + str(pathsDict[elem])
    print "\nConverted Back:"
    reconvertedDict = deepenTagDict(pathsDict)
    for elem in reconvertedDict:
        print elem + ": " + str(reconvertedDict[elem])
    print "\nDone"
# END: def testFlattenDeepen()


def main():
    global c
    serverAddress, whereClause, outputFName = getConfigInfo(readCommandLine())
    print "Accessing server..."
    c = SmapClient(serverAddress)
    print "Querying database..."
    seriesList = getSeriesList(whereClause)
    print "Constructing hierarchy..."
    masterColl = Collection(seriesList)
    print "Getting readings..."
    masterColl.assignReadingsToAll()
    print "Converting data format..."
    constructedDict = masterColl.asFullDict()
    print "Generating JSON file..."
    dictToJson(constructedDict, outputFName)
    print "Done."
# END: def main()

main()
#testFlattenDeepen()
