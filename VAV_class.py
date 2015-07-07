__author__ = 'Miguel'

from smap.archiver.client import SmapClient
import pandas as pd
from pprint import pprint
from ConfigParser import ConfigParser
import json
import quantities as pq
import sys
import VavDataReader


class AHU:
    def __init__(self, SAT_ID, SetPt_ID, coveredVAVs):
        self.uuidSAT = SAT_ID
        self.uuidSetPt = SetPt_ID


def reheatCalcSingle(flowTempValue, sourceTempValue, flowValue=None):
    RHO = 1.2005 * pq.kg/pq.m**3
    C = 1005.0 * pq.J/(pq.kg*pq.degC)
    flowTemp = (flowTempValue * pq.degF).rescale('degC')
    sourceTemp = (sourceTempValue * pq.degF).rescale('degC')
    temp = flowTemp - sourceTemp
    if not flowValue is None:
        flow = flowValue * (pq.ft**3 / pq.minute)
        flow = flow.rescale(pq.CompoundUnit('meter**3/second'))
        calcVal = (temp * flow * RHO * C).rescale('W')
    else:
        calcVal = temp
    return float(calcVal)


# Begin VAV class definition


class VAV:
    def __init__(self, sensors, temp_control_type):
        self.sensors = sensors
        self.temp_control_type = temp_control_type

    def _query_data(self, sensor_name, start_date, end_date, interpolation_time, limit=-1, externalID=None):
        client_obj = SmapClient("http://new.openbms.org/backend")
        if self.sensors.get(sensor_name) is None:
            print 'no ' + sensor_name + ' info'
            return None


        if externalID is None:
            sensorID = self.sensors.get(sensor_name)[0]
        else:
            sensorID = external
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
        return data_table

    # Start rogue pressure function
    def _find_rogue_pressure(self, date_start, date_end, interpolation_time, threshold=95):
        if threshold is None:
            threshold = 95
        table = self._query_data('DMPR_POS', date_start, date_end, interpolation_time)
        if table is None:
            return None
        total = float(table.count())
        count = float(table.where(table[['DMPR_POS']] >= threshold).count())
        percent = (count / total) * 100
        return percent
    # End rogue pressure function

    # Start Rogue Temp heat function
    def _find_rogue_temp_heat(self, date_start, date_end, interpolation_time='5Min', threshold=3):
        if threshold is None:
            threshold = 3
        if self.temp_control_type == 'Dual':
             stpt = self._query_data('HEAT_STPT', date_start, date_end, interpolation_time) + threshold
             roomTemp = self._query_data('ROOM_TEMP', date_start, date_end, interpolation_time)
             if stpt is None:
                 return None
             if roomTemp is None:
                 return None
             total = float(roomTemp.count())
             count = float(roomTemp.where(roomTemp[['ROOM_TEMP']] - stpt[['HEAT_STPT']] >= threshold).count())
             percent = (count / total) * 100
             return percent

        elif self.temp_control_type == 'Single':
             stpt = self._query_data('STPT', date_start, date_end, interpolation_time) + threshold
             roomTemp = self._query_data('ROOM_TEMP', date_start, date_end, interpolation_time)
             if stpt is None:
                 return None
             if roomTemp is None:
                 return None
             total = float(roomTemp.count())
             count = float(roomTemp.where(roomTemp[['ROOM_TEMP']] - stpt[['STPT']] >= threshold).count())
             percent = (count / total) * 100
             return percent

        elif self.temp_control_type == 'Current':
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
            count = float(new_table[['ROOM_TEMP']].where(new_table[['ROOM_TEMP']] - stpt >= threshold).count())
            percent = (count / total) * 100
            return percent

        else:
            print 'unrecognized temperature control type'
    # End Rogue Temp heat function

    # Start Rogue Temp Cool Function
    def _find_rogue_temp_cool(self, date_start, date_end, interpolation_time='5Min', threshold=3):
        if threshold is None:
            threshold = 3
        if self.temp_control_type == 'Dual':
             stpt = self._query_data('COOL_STPT', date_start, date_end, interpolation_time)
             roomTemp = self._query_data('ROOM_TEMP', date_start, date_end, interpolation_time)
             total = float(roomTemp.count())
             count = float(roomTemp.where(stpt[['COOL_STPT']] - roomTemp[['ROOM_TEMP']] >= threshold).count())
             percent = (count / total) * 100
             return percent

        elif self.temp_control_type == 'Single':
             stpt = self._query_data('STPT', date_start, date_end, interpolation_time) - threshold
             roomTemp = self._query_data('ROOM_TEMP', date_start, date_end, interpolation_time)
             total = float(roomTemp.count())
             count = float(roomTemp.where(stpt[['STPT']] - roomTemp[['ROOM_TEMP']] >= threshold).count())
             percent = (count / total) * 100
             return percent

        elif self.temp_control_type == 'Current':
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
            count = float(new_table[['ROOM_TEMP']].where(stpt - new_table[['ROOM_TEMP']] >= threshold).count())
            percent = (count / total) * 100
            return percent

        else:
            print 'unrecognized temperature control type'
    # End Rogue Temp Cool Function

    def find_rogue_temps(self, date_start, date_end, interpolation_time='5Min', threshold=None):
        heats = self._find_rogue_temp_heat(date_start, date_end, interpolation_time, threshold)
        cools = self._find_rogue_temp_cool(date_start, date_end, interpolation_time, threshold)
        return [heats, cools]

    # Start Find Rogue
    def find_rogue(self, rogue_type, threshold=None, date_start='1/1/2014', date_end='now', interpolation_time = '5Min'):
        if rogue_type == 'pressure':
            return self._find_rogue_pressure(date_start, date_end, interpolation_time, threshold)
        elif rogue_type == 'temp_cool':
            return self._find_rogue_temp_cool(date_start, date_end, interpolation_time, threshold)
        elif rogue_type == 'temp_heat':
            return self._find_rogue_temp_heat(date_start, date_end, interpolation_time, threshold)
        else:
            print rogue_type + ' is not a valid option for rogue_type'

    # End Find Rogue

    def calcThermLoad(self, start_date=None, end_date=None, interpolation_time='5min', limit=1000, avgVals=False, sumVals=False, rawVals=False):
        if not (avgVals or sumVals or rawVals):
            print "Warning: no return type marked as True. Defaulting to avgVals."
            avgVals = True
        temprFlowStrDt  = self._query_data('AI_3', start_date, end_date, interpolation_time, limit=limit)
        temprFlowStrDt.columns = ['temprFlow']
        roomTemprStrDt  = self._query_data('ROOM_TEMP', start_date, end_date, interpolation_time, limit=limit)
        roomTemprStrDt.columns = ['roomTempr']
        volAirFlowStrDt = self._query_data('AIR_VOLUME', start_date, end_date, interpolation_time, limit=limit)
        volAirFlowStrDt.columns = ['volAirFlow']

        intermediate = temprFlowStrDt.merge(roomTemprStrDt, right_index=True, left_index=True)
        fullGrouping = intermediate.merge(volAirFlowStrDt, right_index=True, left_index=True)
        
        temprFlowStreamData  = list(fullGrouping['temprFlow'])
        roomTemprStreamData  = list(fullGrouping['roomTempr'])
        volAirFlowStreamData = list(fullGrouping['volAirFlow'])
        
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


    # NOTE: Returns in degrees celcius
    def calcDelta(self, AHU, start_date=None, end_date=None, interpolation_time='5min', limit=1000):
        temprFlowStrDt  = self._query_data('AI_3', start_date, end_date, interpolation_time, limit=limit)
        temprFlowStrDt.columns = ['temprFlow']
        sourceTemprStrDt  = self._query_data(None, start_date, end_date, interpolation_time, limit=limit,  externalID=AHU.uuidSAT)
        sourceTemprStrDt.columns = ['sourceTempr']
        vlvPosStrDt = self._query_data('VLV_POS', start_date, end_date, interpolation_time, limit=limit)
        vlvPosStrDt.columns = ['vlvPos']

        intermediate = temprFlowStrDt.merge(sourceTemprStrDt, right_index=True, left_index=True)
        fullGrouping = intermediate.merge(vlvPosStrDt, right_index=True, left_index=True)
        
        temprFlowStreamData  = list(fullGrouping['temprFlow'])
        sourceTemprStreamData  = list(fullGrouping['sourceTempr'])
        vlvPosStreamData = list(fullGrouping['vlvPosAirFlow'])

        total = 0
        accum = 0

        for f, s, v in zip(temprFlowStreamData, sourceTemprStreamData, vlvPosStreamData):
            if v == 0:
                accum += reheatCalcSingle(f, s)
                total += 1

        if total == 0:
            return 0
        
        return accum / total


    
    def calcReheat(self, AHU, delta, start_date=None, end_date=None, interpolation_time='5min', limit=1000, avgVals=False, sumVals=False, rawVals=False, omitVlvOff=False):
        temprFlowStrDt  = self._query_data('AI_3', start_date, end_date, interpolation_time, limit=limit)
        temprFlowStrDt.columns = ['temprFlow']
        sourceTemprStrDt  = self._query_data(None, start_date, end_date, interpolation_time, limit=limit,  externalID=AHU.uuidSAT)
        sourceTemprStrDt.columns = ['sourceTempr']
        vlvPosStrDt = self._query_data('VLV_POS', start_date, end_date, interpolation_time, limit=limit)
        vlvPosStrDt.columns = ['vlvPos']
        f

        RHO = 1.2005 * pq.kg/pq.m**3
        C = 1005.0 * pq.J/(pq.kg*pq.degC)


def readInput():
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


# Begin Test Script


def testScript(data):
    pressures = pd.DataFrame()
    for key in data.keys():
        inst = VAV(data[key], 'Current')
        value = inst.find_rogue('temp_heat', date_start='4/1/2015', date_end='5/1/2015')
        pressures[key] = [value]


    # testThermLoad = VAV(data['S2-12'], 'Dual')
    # av = testThermLoad.calcRoomThermLoad(None, None, '5min', 10000, 'avg')
    # sm = testThermLoad.calcRoomThermLoad(None, None, '5min', 10000, 'sum')
    # rw = testThermLoad.calcRoomThermLoad(None, None, '5min', 10000, 'raw')
    # print "Avg: " + str(av) + ", Sum: " + str(sm)
    # for t, v in zip(rw['Time'], rw['Value']):
    #     print str(t) + " <<<>>> " + str(v)
    #


    inst = VAV(data['S2-12'], 'Current')  # only for sdj hall
    print inst.find_rogue('temp_heat', None, '4/1/2014', '5/1/2014', '5Min') ## need to find out why removing None messes it up
    print inst.find_rogue('temp_cool', None, '4/1/2014', '5/1/2014', '5Min')
    print inst.find_rogue_temps(date_start='4/1/2014', date_end='5/1/2014')

    testThermLoad = VAV(data['S2-12'], 'Dual')
    print inst.find_rogue('pressure', date_start='4/1/2014', date_end='5/1/2014')
    valsDict = testThermLoad.calcThermLoad(limit=50, avgVals=True, sumVals=True, rawVals=True)
    av = valsDict['Avg']
    sm = valsDict['Sum']
    rw = valsDict['Raw']
    print "Avg: " + str(av) + ", Sum: " + str(sm)
    for t, v in zip(rw['Time'], rw['Value']):
        print str(t) + " <<<>>> " + str(v)


def main():
    inputFileType, inputFileName = readInput()

    if inputFileType == 'j':
        with open(inputFileName) as data_file:
            data = json.load(data_file)
    elif inputFileType == 'c':
        data = VavDataReader.importVavData(inputFileName)

    testScript(data)


main()
