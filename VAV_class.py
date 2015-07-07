__author__ = 'Miguel'

from smap.archiver.client import SmapClient
import pandas as pd
from pprint import pprint
from ConfigParser import ConfigParser
import json
import quantities as pq
import sys


class AHU:
    def __init__(self):
        pass

# Begin VAV class definition

class VAV:
    def __init__(self, sensors, temp_control_type):
        self.sensors = sensors
        self.temp_control_type = temp_control_type

    def _query_data(self, sensor_name, start_date, end_date, interpolation_time, limit=100):
        client_obj = SmapClient("http://new.openbms.org/backend")
        if self.sensors.get(sensor_name) is None:
            print 'no ' + sensor_name + ' info'
            return None
        
        if start_date is None and end_date is None:
            #print 'select data before now limit ' + str(limit) + ' where uuid = \'' + self.sensors.get(sensor_name)[0] + '\''
            x = client_obj.query('select data before now limit ' + str(limit) + ' where uuid = \'' + self.sensors.get(sensor_name)[0] + '\'')
        else:
            #print 'select data in (\'' + start_date + '\', \'' + end_date + '\') limit ' + str(limit) + ' where uuid = \'' + self.sensors.get(sensor_name)[0] + '\''
            x = client_obj.query('select data in (\'' + start_date + '\', \'' + end_date + '\') limit ' + str(limit) + ' where uuid = \'' + self.sensors.get(sensor_name)[0] + '\'')
        pos_table = pd.DataFrame(x[0]['Readings'], columns=['Time', 'Reading'])
        pos_table['Time'] = pd.to_datetime(pos_table['Time'].tolist(), unit='ms').tz_localize('UTC').tz_convert('America/Los_Angeles')
        pos_table.set_index('Time', inplace=True)
        pos_table = pos_table.groupby(pd.TimeGrouper(interpolation_time)).mean().interpolate(method='linear').dropna()
        return pos_table

    # Start rogue pressure function
    def _find_rogue_pressure(self, date_start, date_end, interpolation_time, threshold=95):
        if threshold is None:
            threshold = 95
        table = self._query_data('DMPR_POS', date_start, date_end, interpolation_time)
        if table is None:
            return None
        total = float(table.count())
        count = float(table.where(table[['Reading']] >= threshold).count())
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
             total = float(roomTemp.count())
             count = float(roomTemp.where(roomTemp[['Reading']] - stpt[['Reading']] >= threshold).count())
             percent = (count / total) * 100
             return percent

        elif self.temp_control_type == 'Single':
             stpt = self._query_data('STPT', date_start, date_end, interpolation_time) + threshold
             roomTemp = self._query_data('ROOM_TEMP', date_start, date_end, interpolation_time)
             total = float(roomTemp.count())
             count = float(roomTemp.where(roomTemp[['Reading']] - stpt[['Reading']] >= threshold).count())
             percent = (count / total) * 100
             return percent

        elif self.temp_control_type == 'Current':
            table = self._query_data('HEAT.COOL', date_start, date_end, interpolation_time)
            roomTemp = self._query_data('ROOM_TEMP', date_start, date_end, interpolation_time)
            stpt = int(self._query_data('CTL_STPT', date_start, date_end, interpolation_time).min())
            roomTemp = roomTemp.where(table[['Reading']] == 1)
            total = float(roomTemp.count())
            count = float(roomTemp.where(roomTemp[['Reading']] - stpt >= threshold).count())
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
             count = float(roomTemp.where(stpt[['Reading']] - roomTemp[['Reading']] >= threshold).count())
             percent = (count / total) * 100
             return percent

       

        elif self.temp_control_type == 'Single':
             stpt = self._query_data('STPT', date_start, date_end, interpolation_time) - threshold
             roomTemp = self._query_data('ROOM_TEMP', date_start, date_end, interpolation_time)
             total = float(roomTemp.count())
             count = float(roomTemp.where(stpt[['Reading']] - roomTemp[['Reading']] >= threshold).count())
             percent = (count / total) * 100
             return percent

        elif self.temp_control_type == 'Current':
            table = self._query_data('HEAT.COOL', date_start, date_end, interpolation_time)
            roomTemp = self._query_data('ROOM_TEMP', date_start, date_end, interpolation_time)
            stpt = int(self._query_data('CTL_STPT', date_start, date_end, interpolation_time).max())
            roomTemp = roomTemp.where(table[['Reading']] == 1)
            total = float(roomTemp.count())
            count = float(roomTemp.where(stpt - roomTemp[['Reading']] >= threshold).count())
            percent = (count / total) * 100
            return percent

        else:
            print 'unrecognized temperature control type'
    # End Rogue Temp Cool Function

    def find_rogue_temps(self, date_start, date_end, interpolation_time='5Min', threshold=3):
        heats = self._find_rogue_temp_heat(date_start, date_end, interpolation_time, threshold)
        cools = self._find_rogue_temp_cool(date_start, date_end, interpolation_time, threshold)
        return [heats, cools]

    # Start Find Rogue
    def find_rogue(self, rogue_type, threshold=None, date_start='1/1/2014', date_end='now', interpolation_time = '5Min'):
        if rogue_type == 'Pressure':
            return self._find_rogue_pressure(date_start, date_end, interpolation_time, threshold)
        elif rogue_type == 'Tempc':
            return self._find_rogue_temp_cool(date_start, date_end, interpolation_time, threshold)
        elif rogue_type == 'Temph':
            return self._find_rogue_temp_heat(date_start, date_end, interpolation_time, threshold)
        else:
            print rogue_type + ' is not a valid option for rogue_type'

    # End Find Rogue

    def calcRoomThermLoad(self, start_date=None, end_date=None, interpolation_time='5min', lim=1000, combineType='avg'):
        if not combineType in ['sum', 'avg', 'raw']:
            print "ERROR: combineType value " + combineType + \
                  " not recognised. Exiting."
            sys.exit()


        temprFlowStrDt  = self._query_data('AI_3', start_date, end_date, interpolation_time, limit=lim)
        temprFlowStrDt.columns = ['temprFlow']
        roomTemprStrDt  = self._query_data('ROOM_TEMP', start_date, end_date, interpolation_time, limit=lim)
        roomTemprStrDt.columns = ['roomTempr']
        volAirFlowStrDt = self._query_data('AIR_VOLUME', start_date, end_date, interpolation_time, limit=lim)
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
        

        newList = list([float(e) for e in load])
        if combineType == 'sum':
            retVal = sum(newList)
        elif combineType == 'avg':
            if len(newList) == 0:
                retVal = 0
            else:
                retVal = sum(newList)/float(len(newList))
        elif combineType == 'raw':
            retVal = {'Time':list(fullGrouping.index), 'Value':newList}
        return retVal


# Begin Test Script

# read in the entire json, get as a dict
with open('SDaiLimited.json') as data_file:
    data = json.load(data_file)

pressures = pd.DataFrame()
for key in data.keys():
    inst = VAV(data[key], 'Current')
    value = inst.find_rogue('Pressure', date_start='4/1/2014', date_end='5/1/2014')
    pressures[key] = [value]

print pressures
pressures.plot(kind='hist')
#inst = VAV(data['S2-18'], 'Current')  # only for sdj hall
#print inst.find_rogue('Temph',None, '4/1/2014','5/1/2014', '5Min')
#print inst.find_rogue_temps(date_start='4/1/2014', date_end='5/1/2014')

# testThermLoad = VAV(data['S2-12'], 'Dual')
# av = testThermLoad.calcRoomThermLoad(None, None, '5min', 10000, 'avg')
# sm = testThermLoad.calcRoomThermLoad(None, None, '5min', 10000, 'sum')
# rw = testThermLoad.calcRoomThermLoad(None, None, '5min', 10000, 'raw')
# print "Avg: " + str(av) + ", Sum: " + str(sm)
# for t, v in zip(rw['Time'], rw['Value']):
#     print str(t) + " <<<>>> " + str(v)

