__author__ = 'Miguel'

from smap.archiver.client import SmapClient
import pandas as pd
from pprint import pprint
from ConfigParser import ConfigParser
import json

# Make these class variables into a dictionary to make it concise.
class VAV:
    def __init__(self, sensors):
        self.AI_3 = sensors.get('AI_3')
        self.AIR_VOLUME = sensors.get('AIR_VOLUME')
        self.CLG_LOOPOUT = sensors.get('CLG_LOOPOUT')
        self.CTL_FLOW_MAX = sensors.get('CTL_FLOW_MAX')
        self.CTL_FLOW_MAX_PRI = sensors.get('CTL_FLOW_MAX_PRI')
        self.CTL_FLOW_MIN = sensors.get('CTL_FLOW_MIN')
        self.CTL_FLOW_MIN_PRI = sensors.get('CTL_FLOW_MIN_PRI')
        self.CTL_STPT = sensors.get('CTL_STPT')
        self.CTL_STPT_PRI = sensors.get('CTL_STPT_PRI')
        self.DMPR_POS = sensors.get('DMPR_POS')
        #self.HEAT.COOL = sensors.get('HEAT.COOL')
        #self.HEAT.COOL_PRI = sensors.get('HEAT.COOL_PRI')
        self.HTG_LOOPOUT = sensors.get('HTG_LOOPOUT')
        self.ROOM_TEMP = sensors.get('ROOM_TEMP')
        self.VLV_POS = sensors.get('VLV_POS')
        self.VLV_COMD_PRI = sensors.get('VLV_COMD_PRI')
        self.room = sensors.get('ROOM')

    def find_rogue_pressure(self):
        # gets and sets up the information. Should probably put this in a seperate private method
        client_obj = SmapClient("http://new.openbms.org/backend")
        if self.DMPR_POS is None:
            print 'no DMPR_POS info'
            return
        x = client_obj.query('select data in (\'4/1/2014\', \'4/8/2014\') where uuid = "' + self.DMPR_POS[0] + '"')
        pos_table = pd.DataFrame(x[0]['Readings'], columns=['Time', 'Reading'])
        pos_table['Time'] = pd.to_datetime(pos_table['Time'].tolist(), unit='ms').tz_localize('UTC').tz_convert('America/Los_Angeles')
        pos_table.set_index('Time', inplace=True)
        pos_table = pos_table.groupby(pd.TimeGrouper('5Min')).mean().interpolate(method='linear').dropna()

        # TODO- actually analyze and find the rogue pressures


# key = data['S7-11']

# read in the entire json, get as a dict
with open('SDaiLimited.json') as data_file:
    data = json.load(data_file)

for key in data.keys():
    inst = VAV(data[key])
    inst.find_rogue_pressure()
