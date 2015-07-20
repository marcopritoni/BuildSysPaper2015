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


#####################
#START OPTIONS CLASS#
#####################


class Options:
    @staticmethod
    def assign(qInfo, fInfo, outOptions, dataAttr):
        Options.query = qInfo
        Options.files = fInfo
        Options.output = outOptions
        Options.data = dataAttr

###################
#END OPTIONS CLASS#
###################

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

    ########################
    #START CRITICAL METHODS#
    ########################

    def _getCriticalTable(self, firstFrame, colName1='DMP_POS', colName2=None, second=None, threshold=5, ineq='>=', op1=1):
        combined = False
        if type(second) is pd.DataFrame:
            frm = firstFrame.merge(second, right_index=True, left_index=True, how='inner')
            combined = True
        else:
            frm = firstFrame

        outTable = {'Time':[],'Value':[]}
        for index, row in frm.iterrows():
            firstVal = row[colName1]
            if second is None:
                secondVal = 0
            elif type(second) is pd.DataFrame:
                secondVal = row[colName2]
            else:
                secondVal = second

            if op1 == 1:
                result = eval(str(firstVal) + ' - ' + str(secondVal) + ' ' + ineq + ' ' + str(threshold))
            elif op1 == 2:
                result = eval(str(secondVal) + ' - ' + str(firstVal) + ' ' + ineq + ' ' + str(threshold))

            if result:
                outTable['Value'].append(1)
            else:
                outTable['Value'].append(0)
            outTable['Time'].append(index)

        outFrame = pd.DataFrame(outTable)
        outFrame.set_index('Time', inplace=True)
        return outFrame
        

    # Start critical pressure function
    # Returns the percentage of damper positions that are far outside the expected and desired norm.
    def _find_critical_pressure(self, date_start='4/1/2015', date_end='4/2/2015',
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
        if getAll:
            return self._getCriticalTable(table, colName1='DMPR_POS', colName2=None, second=None, threshold=threshold, ineq='>=', op1=1)      
        total = float(table.count())
        count = float(table.where(table[['DMPR_POS']] >= threshold).count())
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

        ### Query ###
        if self.temp_control_type == 'Dual':
            stptName = 'HEAT_STPT'
        elif self.temp_control_type == 'Single':
            stptName = 'STPT'
        elif self.temp_control_type == 'Current':
            stptName = 'CTL_STPT'
        if useOptions:
            if self.temp_control_type == 'Current':
                table = self._query_data('HEAT.COOL', useOptions=True)
            roomTemp = self._query_data('ROOM_TEMP', useOptions=True)
            stpt = self._query_data(stptName, useOptions=True)
        else:
            if self.temp_control_type == 'Current':
                table = self._query_data('HEAT.COOL', date_start, date_end, interpolation_time)
            roomTemp = self._query_data('ROOM_TEMP', date_start, date_end, interpolation_time)
            stpt = self._query_data('CTL_STPT', date_start, date_end, interpolation_time)
        if self.temp_control_type == 'Current' and table is None:
            return None
        if stpt is None:
            return None
        if roomTemp is None:
            return None
        
        ### Modify ###
        if self.temp_control_type == 'Dual':
            stpt = stpt + threshold
        elif self.temp_control_type == 'Current':
            stpt = int(stpt.min())
            new_table = table.merge(roomTemp, how='outer', left_index=True, right_index=True)
            new_table = new_table.where(new_table[['HEAT.COOL']] == 1, new_table).fillna(new_table[['ROOM_TEMP']].mean())

        ### Output ###
        if getAll:
            if self.temp_control_type == 'Current':
                return self._getCriticalTable(roomTemp, colName1='ROOM_TEMP', second=stpt, colName2=None, threshold=threshold, ineq='>', op1=1)
            else:
                return self._getCriticalTable(roomTemp, colName1='ROOM_TEMP', second=stpt, colName2=stptName, threshold=threshold, ineq='>', op1=1)

        if self.temp_control_type == 'Current':
            total = float(new_table[['ROOM_TEMP']].count())
        else:
            total = float(roomTemp.count())
        if self.temp_control_type == 'Current':
             count = float(new_table[['ROOM_TEMP']].where(new_table[['ROOM_TEMP']] - stpt > threshold).count())
        else:
             count = float(roomTemp.where(roomTemp[['ROOM_TEMP']] - stpt[[stptName]] > threshold).count())
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

        ### Query ###
        if self.temp_control_type == 'Dual':
            stptName = 'COOL_STPT'
        elif self.temp_control_type == 'Single':
            stptName = 'STPT'
        elif self.temp_control_type == 'Current':
            stptName = 'CTL_STPT'
        
        if useOptions:
            if self.temp_control_type == 'Current':
                table = self._query_data('HEAT.COOL', useOptions=True)
            roomTemp = self._query_data('ROOM_TEMP', useOptions=True)
            stpt = self._query_data(stptName, useOptions=True)
        else:
            if self.temp_control_type == 'Current':
                table = self._query_data('HEAT.COOL', date_start, date_end, interpolation_time)
            roomTemp = self._query_data('ROOM_TEMP', date_start, date_end, interpolation_time)
            stpt = self._query_data(stptName, date_start, date_end, interpolation_time)
        if self.temp_control_type == 'Current' and table is None:
            return None
        if stpt is None:
            return None
        if roomTemp is None:
            return None
        
        ### Modify ###
        if self.temp_control_type == 'Current':
            stpt = int(stpt.max())
            new_table = table.merge(roomTemp, how='outer', left_index=True, right_index=True)
            new_table = new_table.where(new_table[['HEAT.COOL']] == 0, new_table).fillna(new_table[['ROOM_TEMP']].mean())
        
        ### Output ###
        if self.temp_control_type == 'Current':
            if getAll:
                return self._getCriticalTable(new_table, colName1='ROOM_TEMP', second=stpt, threshold=threshold, ineq='>', op1=2)
            total = float(new_table[['ROOM_TEMP']].count())
            count = float(new_table[['ROOM_TEMP']].where(stpt - new_table[['ROOM_TEMP']] > threshold).count())
        else:
            if getAll:
                return self._getCriticalTable(stpt, colName1=stptName, second=roomTemp, colName2='ROOM_TEMP', threshold=threshold, ineq='>', op1=1)
            total = float(roomTemp.count())
            count = float(roomTemp.where(stpt[[stptName]] - roomTemp[['ROOM_TEMP']] > threshold).count())

        percent = (count / total) * 100
        return percent
    # End Critical Temp Cool Function

    # Finds both heating and cooling setpoint critical temperature percentages. Returns them as a [heat percentage, cool percentage] pair.
    def find_critical_temps(self, date_start, date_end, interpolation_time='5Min', threshold=None):#, useOptions=False):
        heats = self._find_critical_temp_heat(date_start, date_end, interpolation_time, threshold)
        cools = self._find_critical_temp_cool(date_start, date_end, interpolation_time, threshold)
        return [heats, cools]

    # Start Find Critical
    # Finds critical pressure, heat, or cool, based on critical_type arg. Takes also query info and interpolation time,
    # which it passes to the critical helper functions above
    # TODO (Ian): implement code that handles getAll and inputFrame args.
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
            temprFlowStrDt = inputFrames['Flow Temperature'].copy()
            roomTemprStrDt = inputFrames['Room Temperature'].copy()
            volAirFlowStrDt = inputFrames['Flow Rate'].copy()
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
            temprFlowStrDt = inputFrames['Flow Temperature'].copy()
            sourceTemprStrDt = inputFrames['Source Temperature'].copy()
            vlvPosStrDt = inputFrames['Valve Position'].copy()
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
                temprFlowStrDt = inputFrames['Flow Temperature'].copy()
                sourceTemprStrDt = inputFrames['Source Temperature'].copy()
                vlvPosStrDt = inputFrames['Valve Position'].copy()
                volAirFlowStrDt = inputFrames['Flow Rate'].copy()
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

        RHO = eval(Options.data['rho_val']) * pq.kg/pq.m**3
        C = eval(Options.data['c_val']) * pq.J/(pq.kg*pq.degC)

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
                       'Room Temperature':['ROOM_TEMP'],
                       'Damper Position':['DMPR_POS'],
                       'Heat Set Point':['HEAT_STPT'],
                       'Cool Set Point':['COOL_STPT'],
                       'Set Point':['STPT', 'CTL_STPT']
                       }
    else:
        sensorNames = sensorDict

    qVAV= VAV(None, None, None, None)
    sourceTempr = qVAV._query_data('SOURCE_TEMP', externalID=testAHU.uuidSAT,
                                   useOptions=True)

    frames = {'Source Temperature':sourceTempr}
    
    if VAV_Name is None:
        retDict = {'VAV':[], 'Thermal Load':[],'Delta T':[],
                   'Critical Heat':[], 'Critical Cool':[], 'Critical Pressure':[],
                   'Reheat':[]}
        VAVs = [VAV(key, data[key], Options.data['tempcontroltype'],
                    servAddr) for key in data]
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
        for key in sensorNames:
            shared = list(set(thisVAV.sensors) & set(sensorNames[key]))
            if shared:
                frames[key] = thisVAV._query_data(shared[0], useOptions=True)
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

        def renamecol(frame, newName):
            if type(frame) is pd.DataFrame:
                frame.columns = [newName]
            return frame

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

        #t1 = renamecol(tl, 'Thermal Load')
        #rh = renamecol(rh, 'Reheat')
        #criticalCool = renamecol(criticalCool, 'Critical Cool')
        #criticalHeat = renamecol(criticalHeat, 'Critical Heat')
        #criticalPress = renamecol(criticalPress, 'Critical Pressure')
        #tl.columns = ['Thermal Load']
        #rh.columns = ['Reheat']
        #criticalCool.columns = ['Critical Cool']
        #criticalHeat.columns = ['Critical Heat']
        #criticalPress.columns = ['Critical Pressure']

        #calcs = mergerwrapper(tl, rh)
        #criticalsIntermediate = mergerwrapper(criticalCool, criticalHeat)
        #criticals = mergerwrapper(criticalsIntermediate, criticalPress)
        #newTables = mergerwrapper(calcs, criticals)

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

        #calcs = tl.merge(rh, right_index=True, left_index=True, how='outer')
        #criticalsIntermediate = criticalCool.merge(criticalHeat, right_index=True, left_index=True, how='outer')
        #criticals = criticalsIntermediate.merge(criticalPress, right_index=True, left_index=True, how='outer')

        #fullGroup = calcs.merge(criticals, right_index=True, left_index=True, how='outer')
        return fullGroup

        

        
#def calcReheat(self, ahu=None, delta=None, start_date=None, end_date=None, \
#                   interpolation_time='5min', limit=1000, avgVals=False, \
#                   sumVals=False, rawVals=False, omitVlvOff=False, \
#                   testInput=False, inputFrames=None):
# self, sensor_name, start_date, end_date, interpolation_time, limit=-1, externalID=None


#########################
#END PROCESSING FUNCTION#
#########################


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


#########################
#END INPUT PREPROCESSING#
#########################


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
