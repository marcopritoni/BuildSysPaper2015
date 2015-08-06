import sys
import json
from ConfigParser import ConfigParser


class Options:
    @staticmethod
    def load():
        fName = Options._readinput()
        cDict = Options._readconfig(fName)
        Options.query = cDict['Query']
        Options.files = cDict['IO_Files']
        Options.output = cDict['Output_Options']
        Options.data = cDict['Data_Attributes']

        if Options.data.get('namesjson') is not None:
            with open(Options.data['namesjson']) as f:
                Options.names = json.load(f)
            f.close()
        else:
            Options.names = {'Flow_Temperature':['AI_3'],
                             'Valve_Position':['VLV_POS'],
                             'Flow_Rate':['AIR_VOLUME'],
                             'Room_Temperature':['ROOM_TEMP'],
                             'Damper_Position':['DMPR_POS'],
                             'Heat_Set_Point':['HEAT_STPT'],
                             'Cool_Set_Point':['COOL_STPT'],
                             'Set_Point':['STPT', 'CTL_STPT'],
                             'Heat_Cool':['HEAT.COOL']}
        Options._reverse_names()

    @staticmethod
    def _config_to_dict(cParser):
        cDict = {}
        for section in cParser.sections():
            cDict[section] = {}
            for (key, value) in cParser.items(section):
                cDict[section][key] = value
        return cDict

    @staticmethod
    def _readconfig(configFileName):
        cp = ConfigParser()
        print configFileName
        cp.read(configFileName)
        configDict = Options._config_to_dict(cp)
        for key in configDict:
            subDict = configDict[key]
            for key2 in subDict:
                operItm = subDict[key2]
                if operItm == 'None' or operItm == 'True' or operItm == 'False':
                    subDict[key2] = eval(operItm)
                elif operItm == 'All':
                    subDict[key2] = operItm
                elif len(operItm) > 0 and operItm[0] == '\\':
                    subDict[key2] = operItm[1:]
                elif operItm[0] == '[' and operItm[-1] == ']':
                    subDict[key2] = [x.strip() for x in operItm[1:-1].split(',')]
        return configDict

    @staticmethod
    def _readinput():
        if len(sys.argv) == 1:
            configFileName = raw_input("Please input config file name ==> ")
        elif len(sys.argv) == 2:
            configFileName = sys.argv[1]
        else:
            sys.stderr.write("ERROR: Incorrect number of arguments provided...!\n"
                             "Should be:\n"
                             "python " + sys.argv[0] + " config_file_name\n")
            sys.stderr.flush()
            sys.exit(1)

        return configFileName

    @staticmethod
    def _reverse_names():
        revNames = {}
        for key in Options.names:
            for e in Options.names[key]:
                # print "revNames['" + e + "'] = '" + key + "'"
                revNames[e] = key

        Options.rNames = revNames
