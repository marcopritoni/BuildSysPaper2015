import sys
from smap.archiver.client import SmapClient
import pprint
import fnmatch
import ConfigParser
import json

#class Vav:
#    IDCounter = 1
#    curBuilding = None

#    def __init__(self, building=None):
#        if not building is None and building != Vav.curBuilding:
#            Vav.curBuilding = building
#            Vav.IDCounter = 1
#        if curBuilding is None:
#            "Error: No initial building. Exiting."
#            sys.exit()
#        self.building = Vav.curBuilding
#        self.wcecID = Vav.IDCounter
#        Vav.IDCounter += 1


class SDaiVavParser:
    def checkValidPath(self, p):
        if fnmatch.fnmatchcase(p, '*S[0-9]-[0-9][0-9]*'):
            return True

        return False

    def parsePath(self, p):
        ROOM_ID_SIZE = 5
        
        for i in range(len(p)):
            if fnmatch.fnmatchcase(p[i:i+5], "S[0-9]-[0-9][0-9]"):
                Vav = p[i:i+ROOM_ID_SIZE]
                sensorType = p[i+ROOM_ID_SIZE+1:]
                break
        # roomID = extractRoomID(pointNameStr)
        # sensorType = extractType(pointNameStr)
        
        return Vav, sensorType
# END: class SDaiVavParser


def getConfigInfo(fName):
    cnfg = ConfigParser.ConfigParser()
    cnfg.read(fName)
    serverName = cnfg.get('info', 'server')
    whereClause = cnfg.get('info','whereClause')

    return serverName, whereClause
    


def queryData(serverAddr, whereClause):
    c = SmapClient(serverAddr)
    qList = c.query("select uuid, Path where " + whereClause)
    return qList


def constructData(qList, parser):
    constructed = {}
    for d in qList:
        curPath = d['Path']
        if parser.checkValidPath(curPath):
            curUuid = d['uuid']
            curVav, curSensorType = parser.parsePath(curPath)
            if not curVav in constructed:
                constructed[curVav] = {}
            if not curSensorType in constructed[curVav]:
                constructed[curVav][curSensorType] = []

            constructed[curVav][curSensorType].append(curUuid)

    return constructed
        

def dictToJson(d, outputFile):
    with open(outputFile, 'w') as f:
        json.dump(d, f, indent=5)
    f.close()


def importVavData(configFileName):
    servAddr, whereClause = getConfigInfo(configFileName)
    q = queryData(servAddr, whereClause)
    dataDict = constructData(q, SDaiVavParser())
    dataDict['Server'] = servAddr
    return dataDict
    
    
    
    
