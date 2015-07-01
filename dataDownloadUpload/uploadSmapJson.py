import os
import sys
import ConfigParser

def addrKeyCombine(addr, key):
    if addr[-1] == '/':
        return addr + 'add/' + key
    return addr + '/add/' + key

def getConfigInfo(configName):
    cnfg = ConfigParser.ConfigParser()
    cnfg.read(configName)
    serverAddr = cnfg.get('info', 'serverAddr')
    serverKey = cnfg.get('info','serverKey')
    jsonFile = cnfg.get('info', 'jsonFile')

    return serverAddr, serverKey, jsonFile

def main():
    if len(sys.argv) != 4 and len(sys.argv) != 2:
        print "Wrong number of arguments. Arguments should look like:"
        print "python " + sys.argv[0] + \
              " <server address> <server key> <JSON file>"
        print "or:"
        print "python " + sys.argv[0] + " <config file>"
        print "Quitting."
        sys.exit()
    elif len(sys.argv) == 4:
        serverAddr = sys.argv[1]
        serverKey = sys.argv[2]
        jsonFile = sys.argv[3]
    else:
        serverAddr, serverKey, jsonFile = getConfigInfo(sys.argv[1])

    commandStr = 'curl -XPOST -d @' + \
                 jsonFile + ' -H "Content-Type: application/json" ' + \
                 addrKeyCombine(serverAddr, serverKey)
    print commandStr
    x = raw_input("Proceed? (y or n) ==> ")
    if x[0] == 'y' or x[0] == 'Y':
        print "Uploading..."
        os.system(commandStr)
        print "Done."

main()
