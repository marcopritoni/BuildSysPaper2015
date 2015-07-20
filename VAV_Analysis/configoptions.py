import sys
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
                    subDict[key2] = ALL
                elif len(operItm) > 0 and operItm[0] == '\\':
                    subDict[key2] = operItm[1:]
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
