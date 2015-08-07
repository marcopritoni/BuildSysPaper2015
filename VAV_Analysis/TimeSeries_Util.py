__author__ = 'Miguel'
import pandas as pd
import datetime

def build_table(*args):
    # need to change naming convention of columns for each sensor, to append them and not have same name cols
    table = pd.DataFrame()
    for subtable in args:
        table = table.join(subtable, how='outer')
    return table

def append_rooms(dict):
    # argument must be dictionary with key being the name of the room, and the value being the dataframe
    bigtable = pd.DataFrame()
    for room, table in dict.iteritems():
        table['Room'] = room
        bigtable = bigtable.append(table)
    bigtable = bigtable.reset_index()
    bigtable.set_index(['Room', 'Time'], inplace=True)
    return bigtable

def seperate_periods(datatable, dates):
    rt = pd.DataFrame()
    for start, end in dates:
        rt = rt.append(datatable[start:end])
    return rt