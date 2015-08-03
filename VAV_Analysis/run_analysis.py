"""
Modified on Jul 31 2015
@author: Ian Hurd, Miguel Sanchez, Marco Pritoni
"""
from smap.archiver.client import SmapClient
import pandas as pd
from pprint import pprint
from ConfigParser import ConfigParser
import json
import quantities as pq
import sys
import VavDataReader
import csv
import copy
from configoptions import Options
from Query_data import query_data
from VAV_class_v2 import AHU
from VAV_class_v2 import Sensor
from VAV_class_v2 import VAV
from VAV_class_v2 import rename_sensors




#self, ident, sensors, temp_control_type, serverAddr=None

def processdata(data, servAddr, VAV_Name=None, sensorDict=None):

    testAHU = AHU("a7aa36e6-10c4-5008-8a02-039988f284df",
                  "d20604b8-1c55-5e57-b13a-209f07bc9e0c")

    sensorNames = Options.names

    qSensor = Sensor('Source_Temperature', testAHU.uuidSAT)
    sourceTempr = query_data(qSensor, useOptions=True)

    frames = {'Source_Temperature':sourceTempr}
    
    if VAV_Name is None:
        retDict = {'VAV':[], 'Thermal Load':[],'Delta T':[],
                   'Critical Heat':[], 'Critical Cool':[], 'Critical Pressure':[],
                   'Reheat':[]}
        VAVs = [VAV(key, data[key], Options.data['tempcontroltype'],
                    servAddr) for key in data]
        print "VAV count: " + str(len(VAVs))
        for thisVAV in VAVs:
            print "Processing " + thisVAV.ID
            frames = {'Source_Temperature':sourceTempr}
            
            #for key in sensorNames:
            #    shared = list(set(thisVAV.sensors) & set(sensorNames[key]))
            #    if shared:
            #        frames[key] = query_data(thisVAV, shared[0],
            #                                          useOptions=True)
            #    else:
            #        frames[key] = None
            for key in sensorNames:
                if thisVAV.sensors.get(key) is not None:
                    frames[key] = query_data(thisVAV.sensors[key], useOptions=True)
                else:
                    frames[key] = None
            print "Calculating Thermal Load..."
            tl = thisVAV.calcThermLoad(inputFrames=frames, avgVals=True,
                                       useOptions=True)
            print "Calculating Temperature Delta..."
            dt = thisVAV.calcDelta(inputFrames=frames, useOptions=True)
            print "Calculating Reheat..."
            rh = thisVAV.calcReheat(inputFrames=frames, avgVals=True,
                                    useOptions=True)
            if tl is not None:
                tl = tl['Avg']
            if rh is not None:
                rh = rh['Avg']

            print "Finding Critical Cool..."
            criticalCool  = thisVAV.find_critical('temp_cool', useOptions=True)
            print "Finding Critical Heat..."
            criticalHeat  = thisVAV.find_critical('temp_heat', useOptions=True)
            print "Finding Critical Pressure..."
            criticalPress = thisVAV.find_critical('pressure', useOptions=True)
            
            retDict['Thermal Load'].append(tl)
            retDict['Delta T'].append(dt)
            retDict['Reheat'].append(rh)

            retDict['Critical Heat'].append(criticalHeat)
            retDict['Critical Cool'].append(criticalCool)
            retDict['Critical Pressure'].append(criticalPress)
            retDict['VAV'].append(thisVAV.ID)
            print thisVAV.ID + " complete.\n"
        return pd.DataFrame(retDict)
            
                
    else:
        thisVAV = VAV(VAV_Name, data[VAV_Name],
                      Options.data['tempcontroltype'], servAddr)
        
        #for key in sensorNames:
        #    shared = list(set(thisVAV.sensors) & set(sensorNames[key]))
        #    if shared:
        #        frames[key] = query_data(thisVAV, shared[0], useOptions=True)
        #    else:
        #        frames[key] = None
        for key in sensorNames:
            if thisVAV.sensors.get(key) is not None:
                frames[key] = query_data(thisVAV.sensors[key], useOptions=True)
            else:
                frames[key] = None

        print "Calculating Thermal Load..."
        tl = thisVAV.calcThermLoad(inputFrames=frames, avgVals=True,
                                   rawVals=True, useOptions=True)
        print "Calculating Teperature Delta..."
        dt = thisVAV.calcDelta(inputFrames=frames, useOptions=True)
        print "Calculating Reheat..."
        rh = thisVAV.calcReheat(inputFrames=frames, avgVals=True,
                                rawVals=True, useOptions=True)

        tl = pd.DataFrame(tl['Raw'])
        tl.set_index('Time', inplace=True)
        rh = pd.DataFrame(rh['Raw'])
        rh.set_index('Time', inplace=True)

        print "Finding Critical Cool..."
        criticalCool  = thisVAV.find_critical('temp_cool', getAll=True, useOptions=True)
        print "Finding Critical Heat..."
        criticalHeat  = thisVAV.find_critical('temp_heat', getAll=True, useOptions=True)
        print "Finding Critical Pressure..."
        criticalPress = thisVAV.find_critical('pressure', getAll=True, useOptions=True)

#        def renamecol(frame, newName):
#            if type(frame) is pd.DataFrame:
#                frame.columns = [newName]
#            return frame

        def mergerwrapper(a, b):
            if a is not None and b is not None:
                return a.merge(b, right_index=True, left_index=True, how='outer')
            elif a is not None:
                return a
            elif b is not None:
                return b
            return None

        newDatNames = ['Thermal Load', 'Reheat', 'Critical Cool', 'Critical Heat',
                       'Critical Pressure']

        frames['Thermal Load'] = tl
        frames['Reheat'] = rh
        frames['Critical Cool'] = criticalCool
        frames['Critical Heat'] = criticalHeat
        frames['Critical Pressure'] = criticalPress

        curGroup = None
        for key in frames:
            if frames[key] is not None:
                frames[key].columns = [key]
            curGroup = mergerwrapper(curGroup, frames[key])
        fullGroup = curGroup
        
        for name in newDatNames:
            cols = fullGroup.columns.tolist()
            if name in cols:
                cols.remove(name)
                cols.append(name)
                fullGroup = fullGroup[cols]

        return fullGroup


#########################
#END PROCESSING FUNCTION#
#########################


def main():
    Options.load()

    if Options.files['metadatajson'] is None:
        qStr = 'select ' + Options.query['select'] + ' where ' + Options.query['where']
        data = VavDataReader.importVavData(server=Options.query['client'],
                                           query=qStr)
    else:
        with open(Options.files['metadatajson']) as data_file:
            data = json.load(data_file)
        data_file.close()

    for key in data:
        data[key] = rename_sensors(data[key])
    
    if Options.files['outputjson'] is not None:
        VavDataReader.dictToJson(data, Options.files['outputjson'])

    if Options.files['outputcsv'] is not None or Options.output['printtoscreen']:
        print "Preprocessing finished. Processing now."
        if Options.output['vav'] is None:
            processed = processdata(data, Options.query['client'])
        else:
            processed = processdata(data, Options.query['client'], Options.output['vav'])
        print "Done processing."
        if Options.output['printtoscreen']:
            pd.set_option('display.max_rows', len(processed))
            print processed
            pd.reset_option('display.max_rows')
        if Options.files['outputcsv'] is not None:
           processed.to_csv(Options.files['outputcsv'])
    elif Options.files['outputjson'] is None:
        sys.stderr.write("ERROR: No output specified.\n"
                         "In config file, at least one of the following should"
                         " be true:\n"
                         "- outputJSON is set to something other than None\n"
                         "- outputCSV is set to something other than None\n"
                         "- printToScreen is set to True.\n"
                         "Please modify the config file to satisfy at least "
                         "one of these.\n")
        sys.stderr.flush()
        sys.exit(1)

    print 'Done.'


if __name__ == '__main__':
    main()
