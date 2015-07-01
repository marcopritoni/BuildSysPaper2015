# -*- coding: utf-8 -*-
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
        count = float(table.where[table['Readings'] >= threshold].count())
        percent = (count / total) * 100
        return percent

    def _find_rogue_temp_heat(self, date_start, date_end, interpolation_time, threshold=3):
        if threshold is None:
            threshold = 3
        self._query_data(FILL, date_start, date_end, interpolation_time)
        # TODO- actually analyze and find the rogue temp heat

    def _find_rogue_temp_cool(self, date_start, date_end, interpolation_time, threshold=3):
        self._query_data(FILL, date_start, date_end, interpolation_time)
        # TODO- actually analyze and find the rogue temp cool

    def find_rogue(self, rogue_type, threshold = None, date_start='1/1/2014', date_end='now', interpolation_time = '5Min'):
        if rogue_type == 'pressure':
            self._find_rogue_pressure(date_start, date_end, interpolation_time, threshold)
        elif rogue_type == 'tempc':
            self._find_rogue_temp_cool(date_start, date_end, interpolation_time, threshold)
        elif rogue_type == 'temph':
            self._find_rogue_temp_heat(date_start, date_end, interpolation_time, threshold)
        else:
            print rogue_type + ' is not a valid option for rogue_type'


    def _calcRoomThermLoad(self, temprFlowStreamData, roomTemprStreamData,
                           volAirFlowStreamData, combineType=’sum’):
        RHO = 1.2005 * pq.kg/pq.m**3
        C = 1005 * pq.J/(pq.kg/pq.degC)
        newList = []
        for flowTemprPair, roomTemprPair, flowRatePair in \
            zip(temprFlowStreamData, roomTemprStreamData, volAirFlowStreamData):
            curFlwTmprF = flowTemprPair[1] * pq.degF
            curFlwTemprC = curFlwTemprF.rescale('Deg C')
            curRoomTmprF = roomTemprPair[1] * pq.degF
            curRoomTmprC = curRoomTmprF.rescale('Deg C')
            curTemprDiff = curFlwTmprC - curRoomTmprC
            curFlowRate = flowRatePair[1] * (pq.foot**3 / pq.minute)
            cfrMetric = curFlowRate.rescale(pq.CompountUnit('meter**3/second'))
            curLoad = (curTemprDiff * cfrMetric * RHO * C).rescale('W')
            newList.append(int(curLoad))

        if combineType == 'sum':
            retVal = sum(newList)
        elif combineType == 'avg':
            retVal = sum(newList)/float(len(newList))
        else:
            print "ERROR: Invalid perrameter to _calcRoomThermLoad. Exiting."
            sys.exit()
        
        return retVal
            
            

# read in the entire json, get as a dict
with open('SDaiLimited.json') as data_file:
    data = json.load(data_file)
#
# for key in data.keys():
#     inst = VAV(data[key])
#     inst.find_rogue_pressure()
inst = VAV(data['S2-18'], 'control')
inst.find_rogue('pressure', date_start='4/1/2014', date_end='5/1/2014')

