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
import os.path
from configoptions import Options
from Query_data import query_data
from datetime import datetime
from TimeSeries_Util import build_table, append_rooms, seperate_periods


'''Each instance of this class represents a single sensor.'''
class Sensor:
    def __init__(self, sensorType=None, sensorUUID=None, sensorOwner=None):
        self.sType = sensorType # Type of sensor
        self.uuid = sensorUUID # uuid of sensor time-series
        self.owner = sensorOwner # reference to the VAV, AHU, or other object
                                 # that owns this sensor.

#######################
#START RENAME FUNCTION#
#######################

'''Renames keys in sd to standard names specified.
   dict is returned which is copy of sd except with renamed keys'''
def rename_sensors(sd):
    sDict = copy.deepcopy(sd)
    repCounts = {}
    for key in sDict:
        trueName = Options.rNames.get(key)
        if trueName is not None:
            if trueName in sDict.keys():
                print "REPEAT DETECTED."
            else:
                sDict[Options.rNames[key]] = sDict[key]
                repCounts[trueName] = 1
                del sDict[key]

    return sDict
#####################
#END RENAME FUNCTION#
#####################



# Represents a given AHU
class AHU:
    def __init__(self, SAT_ID, SetPt_ID):
        self.uuidSAT = SAT_ID # UUID of the source air temperature time-series
        self.uuidSetPt = SetPt_ID # UUID of the source set-point time-series.
    

# Begin VAV class definition


class VAV:
    def __init__(self, ident, sensors, temp_control_type, serverAddr=None):
        self.ID = ident # The ID of this VAV
        self.sensors = sensors # A dictionary with sensor-type names as keys, and uuids of these types for the given VAV as values.
        self._make_sensor_objs() # convert self.sensors, as it was read in, to a dict of sensor objects.
        self.temp_control_type = temp_control_type # The type of set point data available for this VAV box
        if serverAddr is None:
            self.serverAddr = "http://new.openbms.org/backend" # Address of the server which contains data for this VAV.
        else:
            self.serverAddr = serverAddr

    def getData(self, sensorObj, start_date='4/1/2015', end_date='4/2/2015', interpolation_time='5min', limit=-1, externalID=None, useOptions=False):
        if useOptions:
            start_date = Options.data['starttime']
            end_date = Options.data['endtime']
            interpolation_time = Options.data['interpolationtime']
            limit = eval(Options.data['limit'])
        if os.path.isfile('Data/' + str(sensorObj.uuid)):
            print 'file detected'
            df = pd.read_csv('Data/' + sensorObj.uuid, index_col=0)
            df.index = pd.to_datetime(df.index.tolist(), unit='ms').tz_localize('UTC').tz_convert('America/Los_Angeles')
            df = df.groupby(pd.TimeGrouper(interpolation_time)).mean().interpolate(method='time').dropna()
            df.columns = [sensorObj.sType]
            return df
        else:
            return query_data(sensorObj, start_date, end_date, interpolation_time, limit, externalID, useOptions)

                   
    '''Converts dict of sensor data to dict of sensor objects'''
    def _make_sensor_objs(self):
        for key in self.sensors:
            self.sensors[key] = Sensor(key, self.sensors[key], self)

    '''Wrapper function for the sensors attribute. Returns empty sensor if
       sensor of type sType not found. Returns actual sensor otherwise.
       This is for the query function, so that it can still print the name of
       the missing sensor.'''
    def getsensor(self, sType):
        x = self.sensors.get(sType)
        if x is None:
            return Sensor(sType)

        return x

        
    ########################
    #START CRITICAL METHODS#
    ########################

    '''Start critical pressure function
       Returns the percentage of damper positions that are far outside the
       expected and desired norm. Setting getAll as True will return an
       in-depth table instead (see _getCriticalTable for more information).'''
    def _find_critical_pressure(self, date_start='4/1/2015', date_end='4/2/2015',
                             interpolation_time='5min', threshold=95, getAll=False, inputFrame=None, useOptions=False):
        if threshold is None and not useOptions:
            threshold = 95
        if useOptions:
            threshold = eval(Options.data['press_threshold'])
            table = self.getData(self.getsensor('Damper_Position'), useOptions=True)
        else:
            table = self.getData(self.getsensor('Damper_Position'), date_start, date_end, interpolation_time)

        if table is None:
            return None

        table['Threshold'] = threshold
        table['Analysis'] = table[['Damper_Position']] >= threshold

        if getAll:
            return table

        total = float(table.count())
        count = float(table[table['Analysis'] == True].count())
        percent = (count / total) * 100

        return percent
    # End critical pressure function

    # Start Critical Temp heat function
    # Returns the percentage of temperatures that are beyond the heating setpoint.
    def _find_critical_temp_heat(self, date_start='4/1/2015', date_end='4/2/2015',
                                 interpolation_time='5Min', threshold=3, getAll=False, inputFrame=None, useOptions=False):
        if self.temp_control_type not in ['Dual' ,'Single', 'Current']:
            print 'unrecognized temperature control type'
            return None

        if threshold is None and not useOptions:
            threshold = 4
        elif useOptions:
            threshold = eval(Options.data['heat_threshold'])

        table = self._get_stpt(date_start, date_end, interpolation_time, 1)
        var = table['Heat_Setpoint'] - table['Room_Temperature']
        table['Temp_Heat_Analysis'] = var > threshold
        if getAll:
            return table
        else:
            total = float(table.count())
            count = float(table[table['Temp_Heat_Analysis'] == True].count())
            percent = (count / total) * 100
            return percent
    # End Critical Temp heat function

    # Start Critical Temp Cool Function
    # Returns the percentage of temperatures that are beyond the cooling setpoint.
    def _find_critical_temp_cool(self, date_start='4/1/2015', date_end='4/2/2015', interpolation_time='5Min', threshold=4, getAll=False, inputFrame=None, useOptions=False):
        if self.temp_control_type not in ['Dual' ,'Single', 'Current']:
            print 'unrecognized temperature control type'
            return None
        
        if threshold is None and not useOptions:
            threshold = 4
        elif useOptions:
            threshold = eval(Options.data['cool_threshold'])

        table = self._get_stpt(date_start, date_end, interpolation_time, 0)
        ### Output ###
        var = table['Cool_Setpoint'] - table['Room_Temperature']
        table['Temp_Cool_Analysis'] = var > threshold
        if getAll:
            return table
        else:
            total = float(table.count())
            count = float(table[table['Temp_Cool_Analysis'] == True].count())
            percent = (count / total) * 100
            return percent
    # End Critical Temp Cool Function

    # Start Find Critical
    # Finds critical pressure, heat, or cool, based on critical_type arg. Takes also query info and interpolation time,
    # which it passes to the critical helper functions above
    # getAll=True makes this return a timeseries of 0 and 1 values (1's representing critical readings)
    # inputFrame (might change this to inputFrames) makes this take in dataframes from already-queried data, rather than querying the data itself.

    def find_critical(self, critical_type, threshold=None, date_start='1/1/2014',
                   date_end='now', interpolation_time = '5Min', getAll=False,
                   inputFrame=None, useOptions=False):
        if critical_type == 'pressure':
            if useOptions:
                return self._find_critical_pressure(inputFrame=inputFrame, getAll=getAll, useOptions=True)
            else:
                return self._find_critical_pressure(date_start, date_end, interpolation_time, threshold, getAll=getAll, inputFrame=inputFrame)
        elif critical_type == 'temp_cool':
            if useOptions:
                return self._find_critical_temp_cool(inputFrame=inputFrame, getAll=getAll, useOptions=True)
            else:
                return self._find_critical_temp_cool(date_start, date_end, interpolation_time, threshold, getAll=getAll, inputFrame=inputFrame)
        elif critical_type == 'temp_heat':
            if useOptions:
                return self._find_critical_temp_heat(inputFrame=inputFrame, getAll=getAll, useOptions=True)
            else:
                return self._find_critical_temp_heat(date_start, date_end, interpolation_time, threshold, getAll=getAll, inputFrame=inputFrame)
        else:
            print critical_type + ' is not a valid option for critical_type'

    def _get_stpt(self, date_start, date_end, interpolation_time, hcv):

        if self.temp_control_type == 'Dual':
            if hcv == 0:
                stptName = 'Cool_Set_Point'
            else:
                stptName = 'Heat_Set_Point'
        elif self.temp_control_type == 'Single':
            stptName = 'Set_Point'
        elif self.temp_control_type == 'Current':
            stptName = 'Set_Point'

        if self.temp_control_type == 'Current':
            table = self.getData(self.getsensor('Heat_Cool'), date_start, date_end, interpolation_time)
        print 'begin Room Temp Query'
        roomTemp = self.getData(self.getsensor('Room_Temperature'), date_start, date_end, interpolation_time)
        stpt = self.getData(self.getsensor(stptName), date_start, date_end, interpolation_time)
        if hcv == 0:
            stptName = 'Cool_Setpoint'
        else:
            stptName = 'Heat_Setpoint'
        stpt.columns = [stptName]
        if self.temp_control_type == 'Current' and table is None:
            return None
        if stpt is None or roomTemp is None:
            return None

        ### Modify ###
        if self.temp_control_type == 'Current':
            stpt = int(stpt.max())
            new_table = table.merge(roomTemp, how='outer', left_index=True, right_index=True)
            new_table[stptName] = stpt
            new_table = new_table.where(new_table[['Heat_Cool']] == hcv, new_table).fillna(new_table[['Room_Temperature']].mean())
        else:
            new_table = roomTemp.merge(stpt, how='outer', left_index=True, right_index=True)
        return new_table

    # End Find Critical

    ######################
    #END CRITICAL METHODS#
    ######################

    ####################
    #START CALC METHODS#
    ####################

    # Called by calcDelta and calcReheat to operate on data, performing unit conversions.
    # If no flow rate or deltaT is given, will calculate deltaT (flow temperature - source temperature).
    # Introducing deltaT and flowValue will perform the full operation.
    def _reheatCalcSingle(self, flowTempValue, sourceTempValue, flowValue=None, deltaT=None):
        RHO = eval(Options.data['rho_val']) * pq.kg/pq.m**3
        C = eval(Options.data['c_val']) * pq.J/(pq.kg*pq.degC)
        flowTemp = (flowTempValue * pq.degF).rescale('degC')
        sourceTemp = (sourceTempValue * pq.degF).rescale('degC')
        if not deltaT is None:
            temp = flowTemp - sourceTemp + (deltaT * pq.degC)
        else:
            temp = flowTemp - sourceTemp
        if not flowValue is None:
            flow = flowValue * (pq.ft**3 / pq.minute)
            flow = flow.rescale(pq.CompoundUnit('meter**3/second'))
            calcVal = (temp * flow * RHO * C).rescale('W')
        else:
            calcVal = temp
        return calcVal


    # Calculates the thermal load of this VAV for timestamps in the range specified, in the interpolation time
    # specified. Outputs as either average of all values calculated, sum of all values calculated, as the
    # series as a whole, or as a combination of the three, depending on which of avgVals, sumVals, or rawVals
    # are set to True.
    def calcThermLoad(self, start_date=None, end_date=None, \
                      interpolation_time='5min', limit=1000, avgVals=False, \
                      sumVals=False, rawVals=False, testInput=False, inputFrames=None, useOptions=False):
        if not (avgVals or sumVals or rawVals):
            print "Warning: no return type marked as True. Defaulting to avgVals."
            avgVals = True

        if inputFrames:
            if inputFrames['Flow_Temperature'] is None or \
               inputFrames['Room_Temperature'] is None or \
               inputFrames['Flow_Rate'] is None:
                return None
            temprFlowStrDt = inputFrames['Flow_Temperature'].copy()
            roomTemprStrDt = inputFrames['Room_Temperature'].copy()
            volAirFlowStrDt = inputFrames['Flow_Rate'].copy()
        else:
            if useOptions:
                temprFlowStrDt  = self.getData(self.getsensor('Flow_Temperature'), useOptions=True)
                roomTemprStrDt  = self.getData(self.getsensor('Room_Temperature'), useOptions=True)
                volAirFlowStrDt = self.getData(self.getsensor('Flow_Rate'), useOptions=True)
            else:
                temprFlowStrDt  = self.getData(self.getsensor('Flow_Temperature'), start_date, end_date, interpolation_time, limit=limit)
                roomTemprStrDt  = self.getData(self.getsensor('Room_Temperature'), start_date, end_date, interpolation_time, limit=limit)
                volAirFlowStrDt = self.getData(self.getsensor('Flow_Rate'), start_date, end_date, interpolation_time, limit=limit)

        temprFlowStrDt.columns  = ['temprFlow']
        roomTemprStrDt.columns  = ['roomTempr']
        volAirFlowStrDt.columns = ['volAirFlow']
        
        intermediate = temprFlowStrDt.merge(roomTemprStrDt, right_index=True, left_index=True, how='outer')
        fullGrouping = intermediate.merge(volAirFlowStrDt, right_index=True, left_index=True, how='outer')
        # fullGrouping = fullGrouping.groupby(pd.TimeGrouper(interpolation_time)).mean().interpolate(method='linear').dropna()
        
        temprFlowStreamData  = list(fullGrouping['temprFlow'])
        roomTemprStreamData  = list(fullGrouping['roomTempr'])
        volAirFlowStreamData = list(fullGrouping['volAirFlow'])
        
        RHO = eval(Options.data['rho_val']) * pq.kg/pq.m**3
        C = eval(Options.data['c_val']) * pq.J/(pq.kg*pq.degC)
        newList = []
        
        flwTmprF = temprFlowStreamData * pq.degF
        flwTmprC = flwTmprF.rescale('degC')
        roomTmprF = roomTemprStreamData * pq.degF
        roomTmprC = roomTmprF.rescale('degC')
        temprDiff = flwTmprC - roomTmprC
        flowRate = volAirFlowStreamData * (pq.foot**3 / pq.minute)
        frMetric = flowRate.rescale(pq.CompoundUnit('meter**3/second'))
        load = (temprDiff * frMetric * RHO * C).rescale('W')
        
        retDict = {}
        newList = list([float(e) for e in load])

        if sumVals:
            retDict['Sum'] = sum(newList)
        if avgVals:
            if len(newList) == 0:
                retDict['Avg'] = 0
            else:
                retDict['Avg'] = sum(newList)/float(len(newList))
        if rawVals:
            retDict['Raw'] = {'Time':list(fullGrouping.index), 'Value':newList}
        return retDict


    # Calculates the difference between source temperature readings and air flow temperature readings from a room's vent. Only does so
    # for readings which coincide with a zero reading for valve-position. Returns the average of results.
    # NOTE: Returns in degrees celcius
    def calcDelta(self, ahu=None, start_date=None, end_date=None, \
                  interpolation_time='5min', limit=1000, testInput=False, inputFrames=None, useOptions=False):
        if testInput:
            temprFlowStrDt  = getCSVFrame('temprFlowTest.csv')
            sourceTemprStrDt  = getCSVFrame('sourceTempr.csv')
            vlvPosStrDt = getCSVFrame('vlvPosTest.csv')
        elif inputFrames:
            if inputFrames['Flow_Temperature'] is None or \
               inputFrames['Source_Temperature'] is None or \
               inputFrames['Valve_Position'] is None:
                return None
            temprFlowStrDt = inputFrames['Flow_Temperature'].copy()
            sourceTemprStrDt = inputFrames['Source_Temperature'].copy()
            vlvPosStrDt = inputFrames['Valve_Position'].copy()
        else:
            assert type(ahu) is AHU
            if useOptions:
                temprFlowStrDt  = self.getData(self.getsensor('Flow_Temperature'), useOptions=True)
                sourceTemprStrDt  = self.getData(None, externalID=ahu.uuidSAT, useOptions=True)
                vlvPosStrDt = self.getData(self.getsensor('Valve_Position'),  useOptions=True)
            else:
                temprFlowStrDt  = self.getData(self.getsensor('Flow_Temperature'), start_date, end_date, interpolation_time, limit=limit)
                sourceTemprStrDt  = self.getData(None, start_date, end_date, interpolation_time, limit=limit, externalID=ahu.uuidSAT)
                vlvPosStrDt = self.getData(self.getsensor('Valve_Position'), start_date, end_date, interpolation_time, limit=limit)


        temprFlowStrDt.columns = ['temprFlow']
        sourceTemprStrDt.columns = ['sourceTempr']
        vlvPosStrDt.columns = ['vlvPos']
        

        intermediate = temprFlowStrDt.merge(sourceTemprStrDt, right_index=True, left_index=True)
        fullGrouping = intermediate.merge(vlvPosStrDt, right_index=True, left_index=True)

        fullGrouping = fullGrouping[fullGrouping['vlvPos'] == 0]
        
        temprFlowStreamData  = list(fullGrouping['temprFlow'])
        sourceTemprStreamData  = list(fullGrouping['sourceTempr'])
        #vlvPosStreamData = list(fullGrouping['vlvPosAirFlow'])

        newList = self._reheatCalcSingle(temprFlowStreamData, sourceTemprStreamData)
        newList = list([float(x) for x in newList])

        if len(newList) == 0:
            return 0.0

        return sum(newList) / len(newList)


    
    def calcReheat(self, ahu=None, delta=None, start_date=None, end_date=None, \
                   interpolation_time='5min', limit=1000, avgVals=False, \
                   sumVals=False, rawVals=False, omitVlvOff=False, \
                   testInput=False, inputFrames=None, useOptions=False):
        if not (avgVals or sumVals or rawVals):
            print "Warning: no return type marked as True. Defaulting to avgVals."
            avgVals = True

        if testInput:
            temprFlowStrDt    = getCSVFrame('temprFlowTest.csv')
            sourceTemprStrDt  = getCSVFrame('sourceTempr.csv')
            vlvPosStrDt       = getCSVFrame('vlvPosTest.csv')
            volAirFlowStrDt   = getCSVFrame('volAirFlowTest.csv')
        elif inputFrames:
                if inputFrames['Flow_Temperature'] is None or \
                   inputFrames['Source_Temperature'] is None or \
                   inputFrames['Valve_Position'] is None or \
                   inputFrames['Flow_Rate'] is None:
                    return None
                temprFlowStrDt = inputFrames['Flow_Temperature'].copy()
                sourceTemprStrDt = inputFrames['Source_Temperature'].copy()
                vlvPosStrDt = inputFrames['Valve_Position'].copy()
                volAirFlowStrDt = inputFrames['Flow_Rate'].copy()
        else:
            assert type(ahu) is AHU and delta is not None
            if useOptions:
                temprFlowStrDt    = self.getData(self.getsensor('Flow_Temperature'), useOptions=True)
                sourceTemprStrDt  = self.getData(None, externalID=ahu.uuidSAT, useOptions=True)
                vlvPosStrDt       = self.getData(self.getsensor('Valve_Position'), useOptions=True)
                volAirFlowStrDt   = self.getData(self.getsensor('Flow_Rate'), useOptions=True)
            else:
                temprFlowStrDt    = self.getData(self.getsensor('Flow_Temperature'), start_date, end_date, interpolation_time, limit=limit)
                sourceTemprStrDt  = self.getData(None, start_date, end_date, interpolation_time, limit=limit,  externalID=ahu.uuidSAT)
                vlvPosStrDt       = self.getData(self.getsensor('Valve_Position'), start_date, end_date, interpolation_time, limit=limit)
                volAirFlowStrDt   = self.getData(self.getsensor('Flow_Rate'), start_date, end_date, interpolation_time, limit=limit)


        temprFlowStrDt.columns   = ['temprFlow']
        sourceTemprStrDt.columns = ['sourceTempr']
        vlvPosStrDt.columns      = ['vlvPos']
        volAirFlowStrDt.columns  = ['volAirFlow']

        interm1 = temprFlowStrDt.merge(sourceTemprStrDt, right_index=True, left_index=True)
        interm2 = volAirFlowStrDt.merge(vlvPosStrDt, right_index=True, left_index=True)
        fullGrouping = interm1.merge(interm2, right_index=True, left_index=True)

        
        if omitVlvOff:
            fullGrouping = fullGrouping[fullGrouping['vlvPos'] != 0]

        RHO = eval(Options.data['rho_val']) * pq.kg/pq.m**3
        C = eval(Options.data['c_val']) * pq.J/(pq.kg*pq.degC)

        temprFlowStreamData    = list(fullGrouping['temprFlow'])
        sourceTemprStreamData  = list(fullGrouping['sourceTempr'])
        volAirFlowStreamData   = list(fullGrouping['volAirFlow'])
        valvePosStreamData     = list(fullGrouping['vlvPos'])
        
        newList = self._reheatCalcSingle(temprFlowStreamData, sourceTemprStreamData, volAirFlowStreamData, delta)
        newList = list([float(x) for x in newList])

        retDict = {}
        if sumVals:
            retDict['Sum'] = sum(newList)
        if avgVals:
            if len(newList) == 0:
                retDict['Avg'] = 0
            else:
                retDict['Avg'] = sum(newList) / float(len(newList))
        if rawVals:
            retDict['Raw'] = {'Time':list(fullGrouping.index), 'Value':newList}
        return retDict
    
    ##################
    #END CALC METHODS#
    ##################



if __name__ == "__main__":
    sensors = {'Valve_Position': '3c1f8ad8-4e7c-5146-8570-f270fb07408c',
           'Flow_Rate': 'e6ac0b13-ef94-5fbe-99ff-01b31a8a259a',
           'CTL_FLOW_MIN':'b7336b08-7d8b-57f2-8826-62cf78a64bed',
           'Damper_Position': '0355e7cc-54ec-5356-9557-161bc436ebc3',
           'CTL_FLOW_MAX': '50afb9ef-2bf5-57f2-8f0a-9de974cd51ed',
           'Heat_Cool': '682e5119-01d6-537f-8413-af0b3253c586',
           'Set_Point': 'd37351e1-f2f3-5d8e-a79a-9c2d518752e2',
           'Room_Temperature': '6394dea9-f31d-5cb1-8f5f-548d0b38d222',
              }

    tmp = VAV('S1-20', sensors, 'Current')
    table = tmp.find_critical('temp_heat', getAll=True)