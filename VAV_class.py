__author__ = 'Miguel'

from smap.archiver.client import SmapClient
import pandas as pd
from pprint import pprint
from ConfigParser import ConfigParser
import json
import quantities as pq
import sys
import VavDataReader
import csv


### START DEBUG FUNCTIONS ###


def primitiveThermLoad(flowTempr, roomTempr, flowRate):
    return (flowTempr - roomTempr) * flowRate * 0.31633653943


def primitiveDelta(flowTempr, sourceTempr):
    return (flowTempr - sourceTempr) * (5.0/9.0)


def primitiveReheat(flowTempr, sourceTempr, flowRate, deltaT):
    t = (flowTempr*(5.0/9.0) - 32) - (sourceTempr*(5.0/9.0) - 32) + deltaT
    return t * flowRate * (1.0/2118.88) * 1.2005 * 1005.0


def getCSVFrame(fileName, interpolation_time='5min'):
    streamList = []
    with open(fileName, 'rb') as inFile:
        f = csv.reader(inFile)
        for row in f:
            eRow = [eval(row[0]) * 1000, eval(row[1])]
            streamList.append(eRow)
    inFile.close()
    #for stream in streamList:
    #    print "HELLO! " + str(stream)
    wrapperDict = {'Readings':streamList}
    q = [wrapperDict]
    

    data_table = pd.DataFrame(q[0]['Readings'], columns=['Time', fileName]) # sensor_name])
    data_table['Time'] = pd.to_datetime(data_table['Time'].tolist(), unit='ms').tz_localize('UTC').tz_convert('America/Los_Angeles')
    data_table.set_index('Time', inplace=True)
    data_table = data_table.groupby(pd.TimeGrouper(interpolation_time)).mean().interpolate(method='linear').dropna()

    return data_table


### END DEBUG FUNCTIONS ###


#######################
#START OPTIONS CLASSES#
#######################


class Options:
    @staticmethod
    def assign(qInfo, fInfo, outOptions, dataAttr):
        Options.query = qInfo
        Options.files = fInfo
        Options.output = outOptions
        Options.data = dataAttr


# NOT CURRENTLY USED #
class InstanceOptions:
    def __init__(self, qInfo, fInfo, outOptions, dataAttr):
        self.query = qInfo
        self.files = fInfo
        self.output = outOptions
        self.data = dataAttr

#####################
#END OPTIONS CLASSES#
#####################

# Represents a given AHU
class AHU:
    def __init__(self, SAT_ID, SetPt_ID):
        self.uuidSAT = SAT_ID
        self.uuidSetPt = SetPt_ID
    

# Begin VAV class definition


class VAV:
    def __init__(self, ident, sensors, temp_control_type, serverAddr=None):
        self.ID = ident
        self.sensors = sensors # A dictionary with sensor-type names as keys, and uuids of these types for the given VAV as values.
        self.temp_control_type = temp_control_type # The type of set point data available for this VAV box
        if serverAddr is None:
            self.serverAddr = "http://new.openbms.org/backend"
        else:
            self.serverAddr = serverAddr

    # Queries for stream data between from a sensor, in user-specified start and end dates and limit.
    # Outputs data as pandas DataFrame object, with data interpolated by interpolation_time.
    # If externalID is given a uuid value, it will query by that ID rather than the one specified by
    # sensor_name.
    def _query_data(self, sensor_name, start_date='4/1/2015',
                    end_date='4/2/2015', interpolation_time='5min', limit=-1,
                    externalID=None, useOptions=False):
        if useOptions:
            start_date = Options.data['starttime']
            end_date = Options.data['endtime']
            interpolation_time = Options.data['interpolationtime']
            limit = eval(Options.data['limit'])
            
        client_obj = SmapClient(self.serverAddr)
        if (self.sensors is None or self.sensors.get(sensor_name) is None) and externalID is None:
            print 'no ' + sensor_name + ' info'
            return None


        if externalID is None:
            sensorID = self.sensors.get(sensor_name)[0]
        else:
            sensorID = externalID
        if start_date is None and end_date is None:
            #print 'select data before now limit ' + str(limit) + ' where uuid = \'' + self.sensors.get(sensor_name)[0] + '\''
            # x = client_obj.query('select data before now limit ' + str(limit) + ' where uuid = \'' + self.sensors.get(sensor_name)[0] + '\'')
            q = client_obj.query('select data before now limit ' + str(limit) + ' where uuid = \'' + sensorID + '\'')
        else:
            #print 'select data in (\'' + start_date + '\', \'' + end_date + '\') limit ' + str(limit) + ' where uuid = \'' + self.sensors.get(sensor_name)[0] + '\''
            q = client_obj.query('select data in (\'' + start_date + '\', \'' + end_date + '\') limit ' + str(limit) + ' where uuid = \'' + sensorID + '\'')

        data_table = pd.DataFrame(q[0]['Readings'], columns=['Time', sensor_name])
        data_table['Time'] = pd.to_datetime(data_table['Time'].tolist(), unit='ms').tz_localize('UTC').tz_convert('America/Los_Angeles')
        data_table.set_index('Time', inplace=True)
        data_table = data_table.groupby(pd.TimeGrouper(interpolation_time)).mean().interpolate(method='linear').dropna()
        # TODO: improve interpolation and dropna()
        return data_table

    #####################
    #START ROGUE METHODS#
    #####################

    # Start rogue pressure function
    # Returns the percentage of damper positions that are far outside the expected and desired norm.
    def _find_rogue_pressure(self, date_start='4/1/2015', date_end='4/2/2015',
                             interpolation_time='5min', threshold=95, getAll=False, inputFrame=None, useOptions=False):
        if threshold is None and not useOptions:
            threshold = 95
        if useOptions:
            threshold = eval(Options.data['press_threshold'])
            table = self._query_data('DMPR_POS', useOptions=True)
        else:
            table = self._query_data('DMPR_POS', date_start, date_end, interpolation_time)
        if table is None:
            return None
        total = float(table.count())
        count = float(table.where(table[['DMPR_POS']] >= threshold).count())
        percent = (count / total) * 100
        return percent
    # End rogue pressure function

    # Start Rogue Temp heat function
    # Returns the percentage of temperatures that are beyond the heating setpoint.
    def _find_rogue_temp_heat(self, date_start='4/1/2015', date_end='4/2/2015', interpolation_time='5Min', threshold=3, getAll=False, inputFrame=None, useOptions=False):
        if threshold is None and not useOptions:
            threshold = 4
        elif useOptions:
            threshold = eval(Options.data['heat_threshold'])

        
        if self.temp_control_type == 'Dual':
             if useOptions:
                 stpt = self._query_data('HEAT_STPT', useOptions=True)
                 roomTemp = self._query_data('ROOM_TEMP', useOptions=True)
             else:
                 stpt = self._query_data('HEAT_STPT', date_start, date_end, interpolation_time)
                 roomTemp = self._query_data('ROOM_TEMP', date_start, date_end, interpolation_time)
             if stpt is None:
                 return None
             if roomTemp is None:
                 return None
             stpt = stpt + threshold
             total = float(roomTemp.count())
             count = float(roomTemp.where(roomTemp[['ROOM_TEMP']] - stpt[['HEAT_STPT']] > threshold).count())
             percent = (count / total) * 100
             return percent

        elif self.temp_control_type == 'Single':
             if useOptions:
                 stpt = self._query_data('STPT', useOptions=True) + threshold
                 roomTemp = self._query_data('ROOM_TEMP', useOptions=True)
             else:
                 stpt = self._query_data('STPT', date_start, date_end, interpolation_time) + threshold
                 roomTemp = self._query_data('ROOM_TEMP', date_start, date_end, interpolation_time)
             if stpt is None:
                 return None
             if roomTemp is None:
                 return None
             total = float(roomTemp.count())
             count = float(roomTemp.where(roomTemp[['ROOM_TEMP']] - stpt[['STPT']] > threshold).count())
             percent = (count / total) * 100
             return percent

        elif self.temp_control_type == 'Current':
            if useOptions:
                table = self._query_data('HEAT.COOL', useOptions=True)
                roomTemp = self._query_data('ROOM_TEMP', useOptions=True)
                stpt = self._query_data('CTL_STPT', useOptions=True)
            else:
                table = self._query_data('HEAT.COOL', date_start, date_end, interpolation_time)
                roomTemp = self._query_data('ROOM_TEMP', date_start, date_end, interpolation_time)
                stpt = self._query_data('CTL_STPT', date_start, date_end, interpolation_time)
            if table is None:
                return None
            if stpt is None:
                 return None
            if roomTemp is None:
                 return None
            stpt = int(stpt.min())
            new_table = table.merge(roomTemp, how='outer', left_index=True, right_index=True)
            new_table = new_table.where(new_table[['HEAT.COOL']] == 1, new_table).fillna(new_table[['ROOM_TEMP']].mean())
            total = float(new_table[['ROOM_TEMP']].count())
            count = float(new_table[['ROOM_TEMP']].where(new_table[['ROOM_TEMP']] - stpt > threshold).count())
            percent = (count / total) * 100
            return percent

        else:
            print 'unrecognized temperature control type'
    # End Rogue Temp heat function

    # Start Rogue Temp Cool Function
    # Returns the percentage of temperatures that are beyond the cooling setpoint.
    def _find_rogue_temp_cool(self, date_start='4/1/2015', date_end='4/2/2015', interpolation_time='5Min', threshold=4, getAll=False, inputFrame=None, useOptions=False):
        if threshold is None and not useOptions:
            threshold = 4
        elif useOptions:
            threshold = eval(Options.data['cool_threshold'])
        
        if self.temp_control_type == 'Dual':
             if useOptions:
                 stpt = self._query_data('COOL_STPT', useOptions=True)
                 roomTemp = self._query_data('ROOM_TEMP', useOptions=True)
             else:
                 stpt = self._query_data('COOL_STPT', date_start, date_end, interpolation_time)
                 roomTemp = self._query_data('ROOM_TEMP', date_start, date_end, interpolation_time)
             if stpt is None or roomTemp is None:
                 return None
             total = float(roomTemp.count())
             count = float(roomTemp.where(stpt[['COOL_STPT']] - roomTemp[['ROOM_TEMP']] > threshold).count())
             percent = (count / total) * 100
             return percent

        elif self.temp_control_type == 'Single':
             if useOptions:
                 stpt = self._query_data('STPT', useOptions=True) - threshold
                 roomTemp = self._query_data('ROOM_TEMP', useOptions=True)
             else:
                 stpt = self._query_data('STPT', date_start, date_end, interpolation_time) - threshold
                 roomTemp = self._query_data('ROOM_TEMP', date_start, date_end, interpolation_time)
             total = float(roomTemp.count())
             count = float(roomTemp.where(stpt[['STPT']] - roomTemp[['ROOM_TEMP']] > threshold).count())
             percent = (count / total) * 100
             return percent

        elif self.temp_control_type == 'Current':
            if useOptions:
                table = self._query_data('HEAT.COOL', useOptions=True)
                roomTemp = self._query_data('ROOM_TEMP', useOptions=True)
                stpt = self._query_data('CTL_STPT', useOptions=True)
            else:
                table = self._query_data('HEAT.COOL', date_start, date_end, interpolation_time)
                roomTemp = self._query_data('ROOM_TEMP', date_start, date_end, interpolation_time)
                stpt = self._query_data('CTL_STPT', date_start, date_end, interpolation_time)

                
            if table is None:
                return None
            if stpt is None:
                 return None
            if roomTemp is None:
                 return None
            stpt = int(stpt.max())
            new_table = table.merge(roomTemp, how='outer', left_index=True, right_index=True)
            new_table = new_table.where(new_table[['HEAT.COOL']] == 0, new_table).fillna(new_table[['ROOM_TEMP']].mean())
            total = float(new_table[['ROOM_TEMP']].count())
            count = float(new_table[['ROOM_TEMP']].where(stpt - new_table[['ROOM_TEMP']] > threshold).count())
            percent = (count / total) * 100
            return percent

        else:
            print 'unrecognized temperature control type'
    # End Rogue Temp Cool Function

    # Finds both heating and cooling setpoint rogue temperature percentages. Returns them as a [heat percentage, cool percentage] pair.
    def find_rogue_temps(self, date_start, date_end, interpolation_time='5Min', threshold=None):#, useOptions=False):
        heats = self._find_rogue_temp_heat(date_start, date_end, interpolation_time, threshold)
        cools = self._find_rogue_temp_cool(date_start, date_end, interpolation_time, threshold)
        return [heats, cools]

    # Start Find Rogue
    # Finds rogue pressure, heat, or cool, based on rogue_type arg. Takes also query info and interpolation time,
    # which it passes to the rogue helper functions above
    # TODO (Ian): implement code that handles getAll and inputFrame args.
    # getAll=True makes this return a timeseries of 0 and 1 values (1's representing rogue readings)
    # inputFrame (might change this to inputFrames) makes this take in dataframes from already-queried data, rather than querying the data itself.
    def find_rogue(self, rogue_type, threshold=None, date_start='1/1/2014',
                   date_end='now', interpolation_time = '5Min', getAll=False,
                   inputFrame=None, useOptions=False):
        if rogue_type == 'pressure':
            if useOptions:
                return self._find_rogue_pressure(inputFrame=inputFrame, useOptions=True)
            else:
                return self._find_rogue_pressure(date_start, date_end, interpolation_time, threshold, inputFrame=inputFrame)
        elif rogue_type == 'temp_cool':
            if useOptions:
                return self._find_rogue_temp_cool(inputFrame=inputFrame, useOptions=True)
            else:
                return self._find_rogue_temp_cool(date_start, date_end, interpolation_time, threshold, inputFrame=inputFrame)
        elif rogue_type == 'temp_heat':
            if useOptions:
                return self._find_rogue_temp_heat(inputFrame=inputFrame, useOptions=True)
            else:
                return self._find_rogue_temp_heat(date_start, date_end, interpolation_time, threshold, inputFrame=inputFrame)
        else:
            print rogue_type + ' is not a valid option for rogue_type'

    # End Find Rogue

    ###################
    #END ROGUE METHODS#
    ###################

    ####################
    #START CALC METHODS#
    ####################

    # Called by calcDelta and calcReheat to operate on data, performing unit conversions.
    # If no flow rate or deltaT is given, will calculate deltaT (flow temperature - source temperature).
    # Introducing deltaT and flowValue will perform the full operation.
    def _reheatCalcSingle(self, flowTempValue, sourceTempValue, flowValue=None, deltaT=None):
        RHO = 1.2005 * pq.kg/pq.m**3
        C = 1005.0 * pq.J/(pq.kg*pq.degC)
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
        ##global flowTemprGlobal
        ##global roomTemprGlobal
        ##global airFlowGlobal
        if not (avgVals or sumVals or rawVals):
            print "Warning: no return type marked as True. Defaulting to avgVals."
            avgVals = True

        if testInput:
            temprFlowStrDt  = getCSVFrame('temprFlowTest.csv')
            roomTemprStrDt  = getCSVFrame('roomTemprTest.csv')
            volAirFlowStrDt = getCSVFrame('volAirFlowTest.csv')
        elif inputFrames:
            if inputFrames['Flow Temperature'] is None or \
               inputFrames['Room Temperature'] is None or \
               inputFrames['Flow Rate'] is None:
                return None
            temprFlowStrDt = inputFrames['Flow Temperature']
            roomTemprStrDt = inputFrames['Room Temperature']
            volAirFlowStrDt = inputFrames['Flow Rate']
        else:
            if useOptions:
                temprFlowStrDt  = self._query_data('AI_3', useOptions=True)
                roomTemprStrDt  = self._query_data('ROOM_TEMP', useOptions=True)
                volAirFlowStrDt = self._query_data('AIR_VOLUME', useOptions=True)
            else:
                temprFlowStrDt  = self._query_data('AI_3', start_date, end_date, interpolation_time, limit=limit)
                roomTemprStrDt  = self._query_data('ROOM_TEMP', start_date, end_date, interpolation_time, limit=limit)
                volAirFlowStrDt = self._query_data('AIR_VOLUME', start_date, end_date, interpolation_time, limit=limit)

        temprFlowStrDt.columns  = ['temprFlow']
        roomTemprStrDt.columns  = ['roomTempr']
        volAirFlowStrDt.columns = ['volAirFlow']
        
        intermediate = temprFlowStrDt.merge(roomTemprStrDt, right_index=True, left_index=True, how='outer')
        fullGrouping = intermediate.merge(volAirFlowStrDt, right_index=True, left_index=True, how='outer')
        # fullGrouping = fullGrouping.groupby(pd.TimeGrouper(interpolation_time)).mean().interpolate(method='linear').dropna()
        # TODO: Additional interpolate
        
        temprFlowStreamData  = list(fullGrouping['temprFlow'])
        roomTemprStreamData  = list(fullGrouping['roomTempr'])
        volAirFlowStreamData = list(fullGrouping['volAirFlow'])

        ##flowTemprGlobal = temprFlowStreamData
        ##roomTemprGlobal = roomTemprStreamData
        ##airFlowGlobal   = volAirFlowStreamData

        
        #temprFlowStreamData  = fullGrouping['temprFlow']
        #roomTemprStreamData  = fullGrouping['roomTempr']
        #volAirFlowStreamData = fullGrouping['volAirFlow']

        
        RHO = 1.2005 * pq.kg/pq.m**3
        C = 1005.0 * pq.J/(pq.kg*pq.degC)
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
        ### Begin Debug Script ###
        #myMappy = map(primitiveThermLoad, temprFlowStreamData, roomTemprStreamData, volAirFlowStreamData)
        #for n, m in zip(newList, myMappy):
        #    uLineSq = '\033[4m'
        #    dStr = ""
        #    if abs(m - n) > 0.1:
        #        dStr += uLineSq
        #    dStr += "Output: " + str(n) + ", Confirmation: " + str(m) + ", Diff: " + str(abs(m - n))
        #    print dStr
        ###  End Debug Script  ###
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
            if inputFrames['Flow Temperature'] is None or \
               inputFrames['Source Temperature'] is None or \
               inputFrames['Valve Position'] is None:
                return None
            temprFlowStrDt = inputFrames['Flow Temperature']
            sourceTemprStrDt = inputFrames['Source Temperature']
            vlvPosStrDt = inputFrames['Valve Position']
        else:
            assert type(ahu) is AHU
            if useOptions:
                temprFlowStrDt  = self._query_data('AI_3', useOptions=True)
                sourceTemprStrDt  = self._query_data(None, externalID=ahu.uuidSAT, useOptions=True)
                vlvPosStrDt = self._query_data('VLV_POS',  useOptions=True)
            else:
                temprFlowStrDt  = self._query_data('AI_3', start_date, end_date, interpolation_time, limit=limit)
                sourceTemprStrDt  = self._query_data(None, start_date, end_date, interpolation_time, limit=limit, externalID=ahu.uuidSAT)
                vlvPosStrDt = self._query_data('VLV_POS',  start_date, end_date, interpolation_time, limit=limit)

            

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
        ### Begin Debug Script ###
        #myMappy = map(primitiveDelta, temprFlowStreamData, sourceTemprStreamData)
        #for n, m in zip(newList, myMappy):
        #    uLineSq = '\033[4m'
        #    dStr = ""
        #    if abs(m - n) > 0.1:
        #        dStr += uLineSq
        #    dStr += "Output: " + str(n) + ", Confirmation: " + str(m) + ", Diff: " + str(abs(m - n))
        #    print dStr
        ###  End Debug Script  ###
        if len(newList) == 0:
            return 0.0

        return sum(newList) / len(newList)

        #total = 0
        #accum = 0

        #for f, s, v in zip(temprFlowStreamData, sourceTemprStreamData, vlvPosStreamData):
        #    if v == 0:
        #        accum += self._reheatCalcSingle(f, s)
        #        total += 1

        #if total == 0:
        #    return 0
        
        #return accum / total


    
    def calcReheat(self, ahu=None, delta=None, start_date=None, end_date=None, \
                   interpolation_time='5min', limit=1000, avgVals=False, \
                   sumVals=False, rawVals=False, omitVlvOff=False, \
                   testInput=False, inputFrames=None, useOptions=False):
        ##global sourceTemprGlobal
        ##global valvePosGlobal
        if not (avgVals or sumVals or rawVals):
            print "Warning: no return type marked as True. Defaulting to avgVals."
            avgVals = True

        if testInput:
            temprFlowStrDt    = getCSVFrame('temprFlowTest.csv')
            sourceTemprStrDt  = getCSVFrame('sourceTempr.csv')
            vlvPosStrDt       = getCSVFrame('vlvPosTest.csv')
            volAirFlowStrDt   = getCSVFrame('volAirFlowTest.csv')
        elif inputFrames:
            if inputFrames['Flow Temperature'] is None or \
               inputFrames['Source Temperature'] is None or \
               inputFrames['Valve Position'] is None or \
               inputFrames['Flow Rate'] is None:
                return None
            temprFlowStrDt = inputFrames['Flow Temperature']
            sourceTemprStrDt = inputFrames['Source Temperature']
            vlvPosStrDt = inputFrames['Valve Position']
            volAirFlowStrDt = inputFrames['Flow Rate']
        else:
            assert type(ahu) is AHU and delta is not None
            if useOptions:
                temprFlowStrDt    = self._query_data('AI_3', useOptions=True)
                sourceTemprStrDt  = self._query_data(None, externalID=ahu.uuidSAT, useOptions=True)
                vlvPosStrDt       = self._query_data('VLV_POS', useOptions=True)
                volAirFlowStrDt   = self._query_data('AIR_VOLUME', useOptions=True)
            else:
                temprFlowStrDt    = self._query_data('AI_3', start_date, end_date, interpolation_time, limit=limit)
                sourceTemprStrDt  = self._query_data(None, start_date, end_date, interpolation_time, limit=limit,  externalID=ahu.uuidSAT)
                vlvPosStrDt       = self._query_data('VLV_POS', start_date, end_date, interpolation_time, limit=limit)
                volAirFlowStrDt   = self._query_data('AIR_VOLUME', start_date, end_date, interpolation_time, limit=limit)

        
        

        temprFlowStrDt.columns   = ['temprFlow']
        sourceTemprStrDt.columns = ['sourceTempr']
        vlvPosStrDt.columns      = ['vlvPos']
        volAirFlowStrDt.columns  = ['volAirFlow']

        interm1 = temprFlowStrDt.merge(sourceTemprStrDt, right_index=True, left_index=True)
        interm2 = volAirFlowStrDt.merge(vlvPosStrDt, right_index=True, left_index=True)
        fullGrouping = interm1.merge(interm2, right_index=True, left_index=True)
        if omitVlvOff:
            fullGrouping = fullGrouping[fullGrouping['vlvPos'] != 0]

        RHO = 1.2005 * pq.kg/pq.m**3
        C = 1005.0 * pq.J/(pq.kg*pq.degC)

        temprFlowStreamData    = list(fullGrouping['temprFlow'])
        sourceTemprStreamData  = list(fullGrouping['sourceTempr'])
        volAirFlowStreamData   = list(fullGrouping['volAirFlow'])
        valvePosStreamData     = list(fullGrouping['vlvPos'])

        ##sourceTemprGlobal = sourceTemprStreamData
        ##valvePosGlobal = valvePosStreamData
        
        #self._reheatCalcSingle(flowTempValue, sourceTempValue, flowValue=None, deltaT=None)
        
        newList = self._reheatCalcSingle(temprFlowStreamData, sourceTemprStreamData, volAirFlowStreamData, delta)
        newList = list([float(x) for x in newList])
        ### Begin Debug Script ###
        #myMappy = map(primitiveReheat, temprFlowStreamData, sourceTemprStreamData, volAirFlowStreamData, \
        #              [delta for i in range(len(temprFlowStreamData))])
        #for n, m in zip(newList, myMappy):
        #    uLineSq = '\033[4m'
        #    dStr = ""
        #    if abs(m - n) > 0.1:
        #        dStr += uLineSq
        #    dStr += "Output: " + str(n) + ", Confirmation: " + str(m) + ", Diff: " + str(abs(m - n))
        #    print dStr
        ###  End Debug Script  ###
        retDict = {}
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
    
    ##################
    #END CALC METHODS#
    ##################


###########################
#BEGIN PROCESSING FUNCTION#
###########################

#self, ident, sensors, temp_control_type, serverAddr=None

def processdata(data, servAddr, VAV_Name=None, sensorDict=None):

    testAHU = AHU("a7aa36e6-10c4-5008-8a02-039988f284df",
                  "d20604b8-1c55-5e57-b13a-209f07bc9e0c")
    
    if sensorDict is None:
        sensorNames = {'Flow Temperature':['AI_3'],
                           'Valve Position':['VLV_POS'],
                           'Flow Rate':['AIR_VOLUME'],
                           'Room Temperature':['ROOM_TEMP']}
    else:
        sensorNames = sensorDict

    qVAV= VAV(None, None, None, None)
    sourceTempr = qVAV._query_data(None, externalID=testAHU.uuidSAT,
                                   useOptions=True)

    frames = {'Source Temperature':sourceTempr}
    
    if VAV_Name is None:
        retDict = {'VAV':[], 'Thermal Load':[],'Delta T':[],
                   'Rogue Heat':[], 'Rogue Cool':[], 'Rogue Pressure':[],
                   'Reheat':[]}
        VAVs = [VAV(key, data[key], 'Current', servAddr) for key in data]
        print "VAV count: " + str(len(VAVs))
        for thisVAV in VAVs:
            print "Processing " + thisVAV.ID
            frames = {'Source Temperature':sourceTempr}
            
            for key in sensorNames:
                shared = list(set(thisVAV.sensors) & set(sensorNames[key]))
                if shared:
                    frames[key] = thisVAV._query_data(shared[0],
                                                      useOptions=True)
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

            print "Finding Rogue Cool..."
            rogueCool  = thisVAV.find_rogue('temp_cool', useOptions=True)
            print "Finding Rogue Heat..."
            rogueHeat  = thisVAV.find_rogue('temp_heat', useOptions=True)
            print "Finding Rogue Pressure..."
            roguePress = thisVAV.find_rogue('pressure', useOptions=True)
            
            retDict['Thermal Load'].append(tl)
            retDict['Delta T'].append(dt)
            retDict['Reheat'].append(rh)

            retDict['Rogue Heat'].append(rogueHeat)
            retDict['Rogue Cool'].append(rogueCool)
            retDict['Rogue Pressure'].append(roguePress)
            retDict['VAV'].append(thisVAV.ID)
            print thisVAV.ID + " complete.\n"
        return pd.DataFrame(retDict)
            
                
    else:
        thisVAV = VAV(VAV_Name, data[VAV_Name], 'dual', servAddr)
        for key in sensorNames:
            shared = list(set(thisVAV.sensors) & set(sensorNames[key]))
            if shared:
                frames[key] = thisVAV._query_data(shared[0], useOptions=True)
            else:
                frames[key] = None

        tl = thisVAV.calcThermLoad(inputFrames=frames, avgVals=True,
                                   rawVals=True, useOptions=True)
        dt = thisVAV.calcDelta(inputFrames=frames, useOptions=True)
        rh = thisVAV.calcReheat(inputFrames=frames, avgVals=True,
                                rawVals=True, useOptions=True)

        print "Finding Rogue Cool..."
        rogueCool  = thisVAV.find_rogue('temp_cool', useOptions=True)
        print "Finding Rogue Heat..."
        rogueHeat  = thisVAV.find_rogue('temp_heat', useOptions=True)
        print "Finding Rogue Pressure..."
        roguePress = thisVAV.find_rogue('pressure', useOptions=True)

        
#def calcReheat(self, ahu=None, delta=None, start_date=None, end_date=None, \
#                   interpolation_time='5min', limit=1000, avgVals=False, \
#                   sumVals=False, rawVals=False, omitVlvOff=False, \
#                   testInput=False, inputFrames=None):
# self, sensor_name, start_date, end_date, interpolation_time, limit=-1, externalID=None


#########################
#END PROCESSING FUNCTION#
#########################


###################
#START TEST SCRIPT#
###################

def testScriptRogue(data):
    
    pressures = pd.DataFrame()
    for key in data.keys():
        if key != 'Server':
            inst = VAV(key, data[key], 'Current', data.get('Server'))
            value = inst.find_rogue('temp_heat', date_start='4/1/2015', date_end='5/1/2015')
            pressures[key] = [value]

    inst = VAV('S2-12', data['S2-12'], 'Current')  # only for sdj hall
    print inst.find_rogue('temp_heat', None, '4/1/2014', '5/1/2014', '5Min')
    print inst.find_rogue('temp_cool', None, '4/1/2014', '5/1/2014', '5Min')
    print inst.find_rogue_temps(date_start='4/1/2014', date_end='5/1/2014')


    print inst.find_rogue('pressure', date_start='4/1/2014', date_end='5/1/2014')


def testScriptCalc(data):
    testThermLoad = VAV('S5-21', data['S5-12'], 'Dual', data.get('Server'))
    valsDict = testThermLoad.calcThermLoad(start_date='6/1/2015', end_date='6/15/2015', limit=-1, avgVals=True, sumVals=True, rawVals=True)#, testInput=True)
    #valsDict = testThermLoad.calcThermLoad(limit=1000, avgVals=True, sumVals=True, rawVals=True)
    av = valsDict['Avg']
    sm = valsDict['Sum']
    rw = valsDict['Raw']
    print "Avg: " + str(av) + ", Sum: " + str(sm)
    #raw_input("Press enter to continue.")

    ##CSVDict = {'Time':rw['Time'], 'ThermalLoad':rw['Value'],'FlowTemperature':flowTemprGlobal, \
    ##           'RoomTemperature':roomTemprGlobal,'AirFlowRate':airFlowGlobal}
    #with open(testThermLoad.ID + '_ThermLoad.csv', 'wb') as outF:
    #    f = csv.writer(outF)
    #    f.writerows(zip(rw['Time'], rw['Value']))
    #outF.close()
    #for t, v in zip(rw['Time'], rw['Value']):
    #    print str(t) + " <<<>>> " + str(v)

    testAHU = AHU("a7aa36e6-10c4-5008-8a02-039988f284df",
                  "d20604b8-1c55-5e57-b13a-209f07bc9e0c")
    deltaT = testThermLoad.calcDelta(testAHU, start_date='6/1/2015', end_date='6/15/2015', interpolation_time='5min', limit=-1)
    print deltaT
    valsDict = testThermLoad.calcReheat(testAHU, deltaT, start_date='6/1/2015', end_date='6/15/2015', limit=-1, avgVals=True, sumVals=True, rawVals=True)
    av = valsDict['Avg']
    sm = valsDict['Sum']
    rw = valsDict['Raw']
    ##CSVDict['SourceTemperature'] = sourceTemprGlobal
    ##CSVDict['ValvePosition'] = valvePosGlobal
    ##CSVDict['Reheat'] = rw['Value']
    print "Reheat:"
    print "Avg: " + str(av) + ", Sum: " + str(sm)
    #raw_input("Press enter to continue.")
    ##with open('calcOutput2.csv', 'wb') as outF:
    ##    w = csv.writer(outF)
    ##    w.writerow(CSVDict.keys())
    ##    w.writerows(zip(*CSVDict.values()))
    ##outF.close()
    #for t, v in zip(rw['Time'], rw['Value']):
    #    print str(t) + " <<<>>> " + str(v)


#################
#END TEST SCRIPT#
#################


###########################
#START INPUT PREPROCESSING#
###########################


# Credit for this subclass goes to
# http://prosseek.blogspot.com/2012/10/reading-ini-file-into-dictionary-in.html
# (Not currently used)
class ConfigToDict(ConfigParser):
    def dictionarify(self):
        d = dict(self._sections)
        for k in d:
            d[k] = dict(self._defaults, **d[k])
            d[k].pop('__name__', None)
        return d


# Switched over to this during debugging.
def configToDict(cParser):
    cDict = {}
    for section in cParser.sections():
        cDict[section] = {}
        for (key, value) in cParser.items(section):
            cDict[section][key] = value
    return cDict
            
        


def readconfig(configFileName):
    cp = ConfigParser()
    print configFileName
    cp.read(configFileName)
    configDict = configToDict(cp)
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
    
    
def readinput():
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


def readInputOld():
    def errPrint(openingMessage):
        print openingMessage
        print "Args should be:"
        print "python " + sys.argv[0] + " -j <filename>"
        print "for json files, and"
        print "python " + sys.argv[0] + " -c <filename>"
        print "for config files."
        print "Exiting."
        
    if len(sys.argv) < 2:
        fType = raw_input("Use (j)son or (c)onfig file? ==> ")
        if fType[0].lower() == 'j':
            fType = 'j'
        elif fType[0].lower() == 'c':
            fType = 'c'
        else:
            print "Your input should start with a 'j' or 'c'. Exiting."
            sys.exit()
        fName = raw_input("Input filename ==> ")
    elif len(sys.argv) == 2:
        errPrint("You must specify which type of file you are using.")
        sys.exit()
    elif len(sys.argv) == 3:
        if sys.argv[1][0] != '-' or \
           (sys.argv[1][1] != 'j' and sys.argv[1][1] != 'c') or \
           len(sys.argv[1]) != 2:
            errPrint("File type specification argument not recognized.")
            sys.exit()
        else:
            fType = sys.argv[1][1]
            fName = sys.argv[2]
    else:
        errPrint("Too many arguments.")
        sys.exit()

    return fType, fName
        
    #if len(sys.argv) != 2:
    #    print "Improper arguments used. Format:"
    #    print "python " + sys.argv[0] + " <config file name>"
    #    sys.exit()

    # return sys.argv[1]


#########################
#END INPUT PREPROCESSING#
#########################


def mainOld():
    # Get input file and its type from user (either through command line        
    # or, if args not supplied, prompt user for them)
    inputFileType, inputFileName = readInputOld()

    # Gather data specified or supplied by file.
    if inputFileType == 'j':
        with open(inputFileName) as data_file:
            data = json.load(data_file)
    elif inputFileType == 'c':
        data = VavDataReader.importVavData(inputFileName)


    #print getCSVFrame("volAirFlowTest.csv", interpolation_time='5min')

    #testScriptRogue(data)
    testScriptCalc(data)


def main():
    configFileName = readinput()
    cDict = readconfig(configFileName)
    queryInfo = cDict['Query']
    fileInfo = cDict['IO_Files']
    outOptions = cDict['Output_Options']
    dataAttr = cDict['Data_Attributes']
    Options.assign(queryInfo, fileInfo, outOptions, dataAttr)

    if fileInfo['metadatajson'] is None:
        qStr = 'select ' + queryInfo['select'] + ' where ' + queryInfo['where']
        data = VavDataReader.importVavData(server=queryInfo['client'],
                                           query=qStr)
    else:
        with open(fileInfo['metadatajson']) as data_file:
            data = json.load(data_file)
        data_file.close()

    if fileInfo['outputjson'] is not None:
        VavDataReader.dictToJson(data, fileInfo['outputjson'])

    if fileInfo['outputcsv'] is not None or outOptions['printtoscreen']:
        print "Preprocessing finished. Processing now."
        if outOptions['vav'] is ALL:
            processed = processdata(data, queryInfo['client'])
        else:
            processed = processdata(data, queryInfo['client'], outOptions['vav'])
        print "Done processing."
        if outOptions['printtoscreen']:
            pd.set_option('display.max_rows', len(processed))
            print processed
            pd.reset_option('display.max_rows')
        if fileInfo['outputcsv'] is not None:
           processed.to_csv(fileInfo['outputcsv'])
    elif fileInfo['outputjson'] is None:
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

ALL = '_A_////_L_////_L_'

main()
