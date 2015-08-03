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
import VAV_class_v2 as vv

'''Each instance of this class represents a single sensor.'''

Options.load()
qStr = 'select ' + Options.query['select'] + ' where ' + Options.query['where']
data = VavDataReader.importVavData(server=Options.query['client'],
                                           query=qStr)
for key in data:
    data[key] = renamine_sensors(data[key])


vvtest=vv.VAV
vv.processdata(data,"http://new.openbms.org/backend")

