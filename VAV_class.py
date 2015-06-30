__author__ = 'Miguel'

from smap.archiver.client import SmapClient
import pandas as pd
from ConfigParser import ConfigParser


class VAV:
    def __init__(self, sensors):
        self.sensor1 = sensors['sensor1']
        self.sensor2 = sensors['sensor2']
        self.sensor3 = sensors['sensor3']
        self.room = None

    def find_rogue_pressure(self):

        SmapClient("http://52.8.2.68:8079/api/query")
