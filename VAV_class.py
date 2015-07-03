__author__ = 'Miguel'

from smap.archiver.client import SmapClient
import pandas as pd
from pprint import pprint
from ConfigParser import ConfigParser
import json
import quantities as pq
import sys

# Make these class variables into a dictionary to make it concise.

class VAV:
    def __init__(self, sensors, temp_control_type):
        self.sensors = sensors
        self.temp_control_type = temp_control_type

    def _query_data(self, sensor_name, start_date, end_date, interpolation_time):
        client_obj = SmapClient("http://new.openbms.org/backend")
        if self.sensors.get(sensor_name) is None:
            print 'no ' + sensor_name + ' info'
            return
        print 'select data in (\'' + start_date + '\', \'' + end_date + '\') where uuid = \'' + self.sensors.get(sensor_name)[0] + '\''
        x = client_obj.query('select data in (\'' + start_date + '\', \'' + end_date + '\') where uuid = \'' + self.sensors.get(sensor_name)[0] + '\'')
        pos_table = pd.DataFrame(x[0]['Readings'], columns=['Time', 'Reading'])
        pos_table['Time'] = pd.to_datetime(pos_table['Time'].tolist(), unit='ms').tz_localize('UTC').tz_convert('America/Los_Angeles')
        pos_table.set_index('Time', inplace=True)
        pos_table = pos_table.groupby(pd.TimeGrouper(interpolation_time)).mean().interpolate(method='linear').dropna()
        return pos_table

    def _find_rogue_pressure(self, date_start, date_end, interpolation_time, threshold=95):
        if threshold is None:
            threshold = 95
        table = self._query_data('DMPR_POS', date_start, date_end, interpolation_time)
        total = float(table.count())
        count = float(table.where(table[['Reading']] >= threshold).count())
        percent = (count / total) * 100
        return percent

    def _find_rogue_temp_heat(self, date_start, date_end, interpolation_time, threshold=3):
        if threshold is None:
            threshold = 3
        if self.temp_control_type == 'Dual':
            print 'nothing'

        elif self.temp_control_type == 'Single':
            print 'nothing'

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


        # TODO- get correct setpoints depending on temp control type
        # TODO- query correctly depending on temp control type
        # TODO- analyze depending on temp control type

    def _find_rogue_temp_cool(self, date_start, date_end, interpolation_time, threshold=3):
        if threshold is None:
            threshold = 3
        if self.temp_control_type == 'Dual':
            print 'nothing'

        elif self.temp_control_type == 'Single':
            print 'nothing'

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
        # TODO- get correct setpoints depending on temp control type
        # TODO- query correctly depending on temp control type
        # TODO- analyze depending on temp control type

    def find_rogue(self, rogue_type, threshold = None, date_start='1/1/2014', date_end='now', interpolation_time = '5Min'):
        if rogue_type == 'Pressure':
            return self._find_rogue_pressure(date_start, date_end, interpolation_time, threshold)
        elif rogue_type == 'Tempc':
            return self._find_rogue_temp_cool(date_start, date_end, interpolation_time, threshold)
        elif rogue_type == 'Temph':
            return self._find_rogue_temp_heat(date_start, date_end, interpolation_time, threshold)
        else:
            print rogue_type + ' is not a valid option for rogue_type'

# _query_data(self, sensor_name, start_date, end_date, interpolation_time)
# (self, temprFlowStreamData, roomTemprStreamData, volAirFlowStreamData, combineType='sum'):

    def _calcRoomThermLoad(self, start_date, end_date, interpolation_time, combineType='sum'):
        temprFlowStreamData  = self._query_data(TEMPR_FLOW_PLACEHOLDER,
                                                start_date, end_date, interpolation_time)['Reading']
        roomTemprStreamData  = self._query_data(ROOM_TEMPR_PLACEHOLDER,
                                                start_date, end_date, interpolation_time)['Reading']
        volAirFlowStreamData = self._query_data(VOL_AIR_FLOW_PLACEHOLDER,
                                                start_date, end_date, interpolation_time)['Reading']
        RHO = 1.2005 * pq.kg/pq.m**3
        C = 1005 * pq.J/(pq.kg/pq.degC)
        newList = []
        for flowTempr, roomTempr, flowRate in \
            zip(temprFlowStreamData, roomTemprStreamData, volAirFlowStreamData):
            curFlwTmprF = flowTempr * pq.degF
            curFlwTmprC = curFlwTmprF.rescale('deg C')
            curRoomTmprF = roomTempr * pq.degF
            curRoomTmprC = curRoomTmprF.rescale('deg C')
            curTemprDiff = curFlwTmprC - curRoomTmprC
            curFlowRate = flowRate * (pq.foot**3 / pq.minute)
            cfrMetric = curFlowRate.rescale(pq.CompountUnit('meter**3/second'))
            curLoad = (curTemprDiff * cfrMetric * RHO * C).rescale('W')
            newList.append(int(curLoad))

        if combineType == 'sum':
            retVal = sum(newList)
        elif combineType == 'avg':
            retVal = sum(newList)/float(len(newList))
        else:
            print "ERROR: Invalid parameter to _calcRoomThermLoad. Exiting."
            sys.exit()
        
        return retVal

# read in the entire json, get as a dict
with open('SDaiLimited.json') as data_file:
    data = json.load(data_file)
#
# for key in data.keys():
#     inst = VAV(data[key])
#     inst.find_rogue_pressure()
inst = VAV(data['S2-18'], 'Current')  # only for sdj hall
print int(inst._query_data('CTL_STPT', '4/1/2014','5/1/2014', '5Min').min())
print inst.find_rogue('Temph',None, '4/1/2014','5/1/2014', '5Min')

