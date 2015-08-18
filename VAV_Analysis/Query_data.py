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

######################
#START QUERY FUNCTION#
######################
# Queries for stream data between from a sensor, in user-specified start and end dates and limit.
# Outputs data as pandas DataFrame object, with data interpolated by interpolation_time.
# If externalID is given a uuid value, it will query by that ID rather than the one specified by
# sensor_name.

'''Queries stream data from the given sensorObj (or uuid externalID if
   specified. Setting useOptions to True will use query options (such as start
   and end times for the stream data) from the global class Options. Options
   can also be called as arguments (but this will likely be phased out in favor
   of the options class).'''
def query_data(sensorObj, start_date='4/1/2015',
               end_date='4/2/2015', interpolation_time='5min', limit=-1,
               externalID=None, useOptions=False):
    if useOptions:
        serverAddr = Options.query['client']
    else:
        serverAddr = sensorObj.owner.serverAddr
    client_obj = SmapClient(serverAddr)
    if (sensorObj is None or sensorObj.uuid is None) and externalID is None:
        if sensorObj is None:
            print "Requested sensor not found."
        else:
            print 'no ' + sensorObj.sType + ' info'
        return None

    if externalID is None:
        sensorID = sensorObj.uuid
    else:
        sensorID = externalID
    if start_date is None and end_date is None:
        q = client_obj.query('select data before now limit ' + str(limit) + ' where uuid = \'' + sensorID + '\'')
    else:
        q = client_obj.query('select data in (\'' + start_date + '\', \'' + end_date + '\') limit ' + str(limit) + ' where uuid = \'' + sensorID + '\'')
    data_table = pd.DataFrame(q[0]['Readings'], columns=['Time', sensorObj.sType])
    data_table['Time'] = pd.to_datetime(data_table['Time'].tolist(), unit='ms').tz_localize('UTC').tz_convert('America/Los_Angeles')
    data_table.set_index('Time', inplace=True)
    data_table = data_table.groupby(pd.TimeGrouper(interpolation_time)).mean().interpolate(method='linear').dropna()
    return data_table


####################
#END QUERY FUNCTION#
####################