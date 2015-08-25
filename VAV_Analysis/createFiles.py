import os
import sys
from smap.archiver.client import SmapClient
from VavDataReader import importVavData
import pandas as pd

qStr = 'select Path, uuid where Path like "%S_-%" and Metadata/SourceName = "Sutardja Dai Hall BACnet"'
validVAVs = importVavData(server='http://www.openbms.org/backend', query=qStr)
interpolation_time = '5T'

c = SmapClient("http://new.openbms.org/backend")

outputDir = "Data"
if len(sys.argv) < 2:
    print "No output directory provided. Using default <Data>"
else:
    outputDir = sys.argv[1].strip()

if os.path.exists(outputDir):
    if not os.path.isdir(outputDir):
        print "File with the same name exists. Delete it first"
        exit()
else:
    os.makedirs(outputDir)

startDate = "6/08/2015"
endDate = "6/09/2015"

numRooms = len(validVAVs)
count = 0
for room in validVAVs:
    count += 1
    print "Pulling in data for room : ", room, "(%d/%d)" % (count, numRooms)
    for sensors in validVAVs[room]:
        for sensor in validVAVs[room][sensors]:
            data = c.query("select data in ('%s','%s') where uuid='%s'" %
                (startDate, endDate, sensor))
            data_table = pd.DataFrame(data[0]['Readings'], columns=['Time', 'Readings'])
            data_table['Time'] = pd.to_datetime(data_table['Time'].tolist(), unit='ms').tz_localize('UTC').tz_convert('America/Los_Angeles')
            data_table.set_index('Time', inplace=True)
            data_table = data_table.resample(interpolation_time)
            data_table.to_csv(outputDir + '/' + sensor)


