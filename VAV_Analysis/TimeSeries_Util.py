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

'''seperate_periods takes in the original datatable from which to parse, and a list of datetime tuples'''


def seperate_periods(datatable, dates):
    # dates must be a list of tuples with first element being start date, second element being end date
    rt = pd.DataFrame()
    for start, end in dates:
        rt = rt.append(datatable[start:end])
    return rt