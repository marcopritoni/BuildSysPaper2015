"""
Modified on Jul 31 2015
@author: Ian Hurd, Miguel Sanchez, Marco Pritoni
"""
import pandas as pd
import matplotlib.pyplot as plt
from sklearn import svm, cross_validation, linear_model, preprocessing
import os.path
import sys
import xlsxwriter

from Query_data import query_data
from VavDataReader import importVavData
from standardizeSensors import standardize
from pint import UnitRegistry

ureg = UnitRegistry()


'''Each instance of this class represents a single sensor.'''
class Sensor:
    def __init__(self, sensorType=None, sensorUUID=None, sensorOwner=None):
        self.sType = sensorType # Type of sensor
        self.uuid = sensorUUID # uuid of sensor time-series
        self.owner = sensorOwner # reference to the VAV, AHU, or other object
                                 # that owns this sensor.

# Represents a given AHU
class AHU:
    def __init__(self, SAT_ID, SetPt_ID, serverAddr=None):
        self.sensors = Sensor('Source_Temperature', SAT_ID, self)
        self.uuidSAT = SAT_ID  # UUID of the source air temperature time-series
        self.uuidSetPt = SetPt_ID  # UUID of the source set-point time-series.
        if serverAddr is None:
            self.serverAddr = "http://new.openbms.org/backend" # Address of the server which contains data for this VAV.
        else:
            self.serverAddr = serverAddr
    

# Begin VAV class definition
class VAV:
    qStr = 'select Path, uuid where Path like "%S_-%" and Metadata/SourceName = "Sutardja Dai Hall BACnet"'
    validVAVs = importVavData(server='http://www.openbms.org/backend', query=qStr)

    def __init__(self, ident, temp_control_type, rho=1.2005, spec_heat=1005.0, serverAddr=None):
        try:
            sensors = VAV.validVAVs[ident]
        except Exception as e:
            sys.exit(str(e) + ' was not found in list of valid VAV names')
        self.ID = ident     # The ID of this VAV
        self.sensors = standardize(sensors) # A dictionary with sensor-type names as keys, and uuids of these types for the given VAV as values.
        self._make_sensor_objs() # convert self.sensors, as it was read in, to a dict of sensor objects.
        self.temp_control_type = temp_control_type  # The type of set point data available for this VAV box
        self.rho = ureg.Quantity(rho,'kg/meter**3')
        self.specific_heat = ureg.Quantity(spec_heat, 'J/(kg*degC)')
        if serverAddr is None:
            self.serverAddr = "http://new.openbms.org/backend" # Address of the server which contains data for this VAV.
        else:
            self.serverAddr = serverAddr

    def getData(self, sensorObj, start_date='4/1/2015', end_date='4/2/2015', interpolation_time='5T', limit=-1, externalID=None):
        if os.path.isfile('Data/' + str(sensorObj.uuid)):
            print 'file detected for ' + sensorObj.sType + ': ' + sensorObj.uuid
            df = pd.read_csv('Data/' + sensorObj.uuid, index_col=0)
            if df.empty:
                sys.exit('No Data for ' + sensorObj.sType + ' for the given time period')
            df.index = pd.to_datetime(df.index.tolist(), unit='ms').tz_localize('UTC').tz_convert('America/Los_Angeles')
            df = df.groupby(pd.TimeGrouper(interpolation_time)).mean().interpolate(method='time').dropna()
            df.columns = [sensorObj.sType]
            return df
        else:
            return query_data(sensorObj, start_date, end_date, interpolation_time, limit, externalID)

                   
    '''Converts dict of sensor data to dict of sensor objects'''
    def _make_sensor_objs(self):
        for key in self.sensors:
            self.sensors[key] = Sensor(key, self.sensors[key], self)

    '''Wrapper function for the sensors attribute. Returns empty sensor if
       sensor of type sType not found. Returns actual sensor otherwise.
       This is for the query function, so that it can still print the name of
       the missing sensor.'''
    def getsensor(self, sType):
        x = self.sensors.get(sType)
        if x is None:
            sys.exit(sType + ' sensor does not exist for ' + str(self.ID))

        return x

        
    ########################
    #START CRITICAL METHODS#
    ########################

    '''Start critical pressure function
       Returns the percentage of damper positions that are far outside the
       expected and desired norm. Setting getAll as True will return an
       in-depth table instead (see _getCriticalTable for more information).'''
    def find_critical_pressure(self, date_start='4/1/2015', date_end='4/2/2015',
                             interpolation_time='5T', threshold=95, getAll=False):

        table = self.getData(self.getsensor('Damper_Position'), date_start, date_end, interpolation_time)

        if table is None:
            return None

        table['Threshold'] = threshold
        table['Analysis'] = table[['Damper_Position']] >= threshold

        if getAll:
            return table

        percent = table['Analysis'].mean()

        return percent
    # End critical pressure function

    # Start Critical Temp heat function
    # Returns the percentage of temperatures that are beyond the heating setpoint.
    def find_critical_temp_heat(self, date_start='4/1/2015', date_end='4/2/2015',
                                 interpolation_time='5T', threshold=3, getAll=False, offset=0):
        if self.temp_control_type not in ['Dual', 'Single', 'Current']:
            print 'unrecognized temperature control type'
            return None

        table = self._get_stpt(date_start, date_end, interpolation_time, 'heat', offset)
        if table is None:
            return None
        diff = table['Heat_Setpoint'] - table['Room_Temperature']
        table['Temp_Heat_Analysis'] = diff > threshold

        if getAll:
            return table
        else:
            percent = table['Temp_Heat_Analysis'].mean()
            return percent
    # End Critical Temp heat function

    # Start Critical Temp Cool Function
    # Returns the percentage of temperatures that are beyond the cooling setpoint.
    def find_critical_temp_cool(self, date_start='4/1/2015', date_end='4/2/2015', interpolation_time='5T', threshold=4, getAll=False, offset=0):

        if self.temp_control_type not in ['Dual' ,'Single', 'Current']:
            print 'unrecognized temperature control type'
            return None

        table = self._get_stpt(date_start, date_end, interpolation_time, 'cool', offset)

        if table is None:
            return None

        diff = table['Room_Temperature'] - table['Cool_Setpoint']
        table['Temp_Cool_Analysis'] = diff > threshold
        if getAll:
            return table
        else:
            percent = table['Temp_Cool_Analysis'].mean()
            return percent
    # End Critical Temp Cool Function

    def _get_stpt(self, date_start, date_end, interpolation_time, hcv, offset):
        if hcv == 'cool':
            hcv = 0
        elif hcv == 'heat':
            hcv = 1
        if self.temp_control_type == 'Dual':
            if hcv == 0:
                stptName = 'Cool_Set_Point'
            else:
                stptName = 'Heat_Set_Point'
        elif self.temp_control_type == 'Single':
            stptName = 'Set_Point'
        elif self.temp_control_type == 'Current':
            stptName = 'Set_Point'

        if self.temp_control_type == 'Current':
            table = self.getData(self.getsensor('Heat_Cool'), date_start, date_end, interpolation_time)
        roomTemp = self.getData(self.getsensor('Room_Temperature'), date_start, date_end, interpolation_time)
        stpt = self.getData(self.getsensor(stptName), date_start, date_end, interpolation_time)
        if hcv == 0:
            stptName = 'Cool_Setpoint'
            if self.temp_control_type == 'Single':
                stpt.stptName += offset
        else:
            stptName = 'Heat_Setpoint'
            if self.temp_control_type == 'Single':
                stpt.stptName -= offset
        stpt.columns = [stptName]
        if self.temp_control_type == 'Current' and table is None:
            return None
        if stpt is None or roomTemp is None:
            return None

        ### Modify ###
        if self.temp_control_type == 'Current':
            stpt = int(stpt.max())
            new_table = table.merge(roomTemp, how='outer', left_index=True, right_index=True)
            new_table[stptName] = stpt

            new_table = new_table[new_table['Heat_Cool'] == hcv]
            if new_table.empty:
                return None
        else:
            new_table = roomTemp.merge(stpt, how='outer', left_index=True, right_index=True)
        return new_table

    ######################
    #END CRITICAL METHODS#
    ######################

    ####################
    #START CALC METHODS#
    ####################

    # Called by calcDelta and calcReheat to operate on data, performing unit conversions.
    # If no flow rate or deltaT is given, will calculate deltaT (flow temperature - source temperature).
    # Introducing deltaT and flowValue will perform the full operation.
    def _reheatCalcSingle(self, flowTempValue, sourceTempValue, flowValue=None, deltaT=None):
        if deltaT is not None:
            temp = (flowTempValue['Flow_Temperature'] - sourceTempValue['Source_Temperature']) - (deltaT)
        else:
            temp = flowTempValue['Flow_Temperature'] - sourceTempValue['Source_Temperature']
        temp = pd.DataFrame(temp, columns=['Temp_Diff'])
        if flowValue is not None:
            calcVal = self._unit_conv(pd.DataFrame((temp['Temp_Diff'] * flowValue['Flow_Rate'] * self.rho.magnitude
                                                    * self.specific_heat.magnitude),
                                   columns=['RV'], index=temp.index),'degC*meter**3/second * kg/meter**3 * J/(kg*degC)',
                                      'watt')
        else:
            calcVal = temp
        return calcVal


    # Calculates the thermal load of this VAV for timestamps in the range specified, in the interpolation time
    # specified. Outputs as either average of all values calculated, sum of all values calculated, as the
    # series as a whole, or as a combination of the three, depending on which of avgVals, sumVals, or rawVals
    # are set to True.
    def calcThermLoad(self, start_date='4/1/2015', end_date='4/2/2015', interpolation_time='5T', limit=1000):

        temprFlowStrDt = self._unit_conv(self.getData(self.getsensor('Flow_Temperature'), start_date, end_date,
                                                      interpolation_time, limit=limit), 'degF', 'degC')
        roomTemprStrDt  = self._unit_conv(self.getData(self.getsensor('Room_Temperature'), start_date, end_date,
                                                       interpolation_time, limit=limit), 'degF', 'degC')
        volAirFlowStrDt = self._unit_conv(self.getData(self.getsensor('Flow_Rate'), start_date, end_date,
                                            interpolation_time, limit=limit), 'ft**3/minute', 'meter**3/second')

        temprDiff = pd.DataFrame()
        temprDiff['Temp_Diff'] = temprFlowStrDt['Flow_Temperature'] - roomTemprStrDt['Room_Temperature']
        load = self._unit_conv(pd.DataFrame((temprDiff['Temp_Diff']  * volAirFlowStrDt['Flow_Rate'] *
                             self.rho.magnitude * self.specific_heat.magnitude), columns=['RV'], index=temprDiff.index),
                               'degC*meter**3/second * kg/meter**3 * J/(kg*degC)', 'watt')
        return self._produceOutput(volAirFlowStrDt.join([temprDiff, load], how='outer'))

    # Calculates the difference between source temperature readings and air flow temperature readings from a room's vent. Only does so
    # for readings which coincide with a zero reading for valve-position. Returns the average of results.
    # NOTE: Returns in degrees celcius
    def calcDelta(self, ahu=None, start_date=None, end_date=None,
                  interpolation_time='5T', limit=1000):

        assert ahu.__class__ is AHU

        temprFlowStrDt = self._unit_conv(self.getData(self.getsensor('Flow_Temperature'), start_date, end_date,
                                                      interpolation_time, limit=limit), 'degF', 'degC')
        sourceTemprStrDt = self._unit_conv(self.getData(ahu.sensors, start_date, end_date, interpolation_time,
                                        limit=limit), 'degF', 'degC')
        vlvPosStrDt = self.getData(self.getsensor('Valve_Position'), start_date, end_date, interpolation_time, limit=limit)

        fullGrouping = temprFlowStrDt.join([sourceTemprStrDt, vlvPosStrDt],how='outer')
        fullGrouping = fullGrouping[fullGrouping['Valve_Position'] == 0]

        newList = self._reheatCalcSingle(fullGrouping[['Flow_Temperature']], fullGrouping[['Source_Temperature']])
        return newList.mean().values

    def calcReheat(self, ahu=None, delta=None, start_date='4/1/2015', end_date='4/2/2015', interpolation_time='5T',
                   limit=1000):

        assert ahu.__class__ is AHU
        temprFlowStrDt = self._unit_conv(self.getData(self.getsensor('Flow_Temperature'), start_date, end_date, interpolation_time,
                                         limit=limit), 'degF', 'degC')
        sourceTemprStrDt = self._unit_conv(self.getData(ahu.sensors, start_date, end_date, interpolation_time,
                                        limit=limit), 'degF', 'degC')
        volAirFlowStrDt = self._unit_conv(self.getData(self.getsensor('Flow_Rate'), start_date, end_date,
                                            interpolation_time, limit=limit), 'ft**3/minute', 'meter**3/second')

        vlvPosStrDt = self.getData(self.getsensor('Valve_Position'), start_date, end_date, interpolation_time, limit=limit)
        if delta is None:
            delta = self.calcDelta(ahu, start_date, end_date, interpolation_time, limit)

        newList = self._reheatCalcSingle(temprFlowStrDt, sourceTemprStrDt, volAirFlowStrDt, delta)
        return self._produceOutput(temprFlowStrDt.join([sourceTemprStrDt, volAirFlowStrDt, newList, vlvPosStrDt], how='outer'))

    @staticmethod
    def _unit_conv(dataframe, initial_unit, final_unit):
        dataframe[dataframe.columns[0]] = ureg.Quantity(dataframe.values, initial_unit).to(final_unit).magnitude
        return dataframe

    def linear_regression(self, data, target):
        [data_train, data_valid, target_train,target_valid] = self._split_data(data,target)
        regr = linear_model.LinearRegression()
        regr.fit(data_train, target_train)
        regr.score(data_valid, target_valid)

    def plot_prediction(self, model,actual ):
        plt.plot(model.predict(actual), color = 'blue')
        plt.plot(actual, color='red')
        plt.title(' Predicted(blue) vs Actual(red)')
        plt.show

    @staticmethod
    def _produceOutput(newList):
        retDict = {}
        retDict['Avg'] = newList['RV'].mean()
        retDict['Raw'] = newList
        return retDict

    @staticmethod
    def _split_data(data,target):
        #Figure out how we want to split the data
        print 'hello world'

    
    ##################
    #END CALC METHODS#
    ##################
if __name__ == "__main__":
    chosen = 'S5-01'

    testAHU = AHU("a7aa36e6-10c4-5008-8a02-039988f284df",
                  "d20604b8-1c55-5e57-b13a-209f07bc9e0c",)
    tmp = VAV(chosen, 'Current')
    critHeat = tmp.find_critical_temp_heat(date_start='6/8/2015',date_end='6/9/2015', getAll=True)
    critHeat.to_csv(chosen + '_heat.csv')
    critCool = tmp.find_critical_temp_cool(date_start='6/8/2015',date_end='6/9/2015', getAll=True)
    critCool.to_csv(chosen + '_cool.csv')
    rtrn = tmp.calcReheat(testAHU, start_date='6/8/2015', end_date='6/9/2015')
    rtrn['Raw'].to_csv(chosen + '_raw.csv')

