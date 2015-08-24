__author__ = 'Miguel'

sens_dict = {'AI_3': 'Flow_Temperature',
             'VLV_POS': 'Valve_Position',
             'AIR_VOLUME': 'Flow_Rate',
             'ROOM_TEMP': 'Room_Temperature',
             'DMPR_POS': 'Damper_Position',
             'HEAT_STPT': 'Heat_Set_Point',
             'COOL_STPT': 'Cool_Set_Point',
             'STPT': 'Set_Point',
             'CTL_STPT': 'Set_Point',
             'HEAT.COOL': 'Heat_Cool'
             }


def standardize(sensors):
    new_dict = {}
    for key in sensors:
        try:
            new_dict[sens_dict[key]] = sensors[key]
        except KeyError as e:
            print 'could not standardize ' + str(e)
    return new_dict