"""
Modified on Jul 31 2015
@author: Ian Hurd, Miguel Sanchez, Marco Pritoni
"""
import pandas as pd
from configoptions import Options
from VAV_class_v2 import AHU
from VAV_class_v2 import VAV


def processdata(servAddr, VAV_Name=None):
    bad = []
    start_date = "6/08/2015"
    end_date = "6/09/2015"
    count = 0
    all_analysis = pd.DataFrame()
    testAHU = AHU("a7aa36e6-10c4-5008-8a02-039988f284df",
                  "d20604b8-1c55-5e57-b13a-209f07bc9e0c")
    retDict = {'VAV':[], 'Thermal Load':[],
               'Critical Heat':[], 'Critical Cool':[], 'Critical Pressure':[],
               'Reheat':[]}
    if VAV_Name == 'All':
        VAVs = [VAV(key, Options.data['tempcontroltype'],
                serverAddr=servAddr) for key in VAV.validVAVs]
    elif type(VAV_Name) is list:
        VAVs = [VAV(item, Options.data['tempcontroltype'], serverAddr=servAddr) for item in VAV_Name]
    else:
        VAVs = [VAV(VAV_Name, Options.data['tempcontroltype'], serverAddr=servAddr)]
    print "VAV count: " + str(len(VAVs))
    for thisVAV in VAVs:
        try:
            print "Processing " + thisVAV.ID
            print "Calculating Thermal Load..."
            tl = thisVAV.calcThermLoad(start_date=start_date,end_date=end_date)
            print "Calculating Reheat..."
            rh = thisVAV.calcReheat(testAHU, start_date=start_date,end_date=end_date)
            if tl is not None:
                tl = tl['Avg']
            if rh is not None:
                rh = rh['Avg']
            print "Finding Critical Cool..."
            criticalCool  = thisVAV.find_critical_temp_cool(date_start=start_date, date_end=end_date)
            print "Finding Critical Heat..."
            criticalHeat  = thisVAV.find_critical_temp_heat(date_start=start_date, date_end=end_date)
            print "Finding Critical Pressure..."
            criticalPress = thisVAV.find_critical_pressure(date_start=start_date, date_end=end_date)
        except ValueError as e:
            bad.append(thisVAV.ID)
            print thisVAV.ID + ' does not have necessary data ' + str(e)
            continue
        except SystemExit:
            bad.append(thisVAV.ID)
            continue
        count += 1
        retDict['Thermal Load'].append(tl)
        retDict['Reheat'].append(rh)

        retDict['Critical Heat'].append(criticalHeat)
        retDict['Critical Cool'].append(criticalCool)
        retDict['Critical Pressure'].append(criticalPress)
        retDict['VAV'].append(thisVAV.ID)
        print thisVAV.ID + " complete.\n"
        all_analysis = all_analysis.append(pd.DataFrame(retDict), ignore_index=True)
    all_analysis.set_index('VAV', inplace=True)
    return [all_analysis, bad]


#########################
#END PROCESSING FUNCTION#
#########################


def main():
    Options.load()
    processed, bad = processdata(Options.query['client'], Options.output['vav'])
    print "Done processing."
    print bad
    processed.to_csv('Run_Analysis_Output.csv')

    print 'Done.'


if __name__ == '__main__':
    main()
