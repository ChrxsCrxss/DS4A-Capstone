import pandas as pd
import re
import states_dictionary as states_dict
import utilities

import sys
import subprocess

subprocess.check_call([sys.executable, '-m', 'pip', 'install',
                       'xlrd'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install',
                       'openpyxl'])


def run():
    df_unemployment = pd.read_excel('../../data/raw/Unemployment.xls')

    # Rename columns to the fieds in row 3 then rename two of those columns to state and county
    df_unemployment = df_unemployment.rename(
        columns=df_unemployment.iloc[3])
    df_unemployment = df_unemployment.rename(
        columns={'Stabr': 'state', 'area_name': 'county'})

    # drop the first 4 rows that contain all Nan values and/or column headers
    df_unemployment = df_unemployment.drop([0, 1, 2, 3])

    # Reset index to begin with 0 and drop previous index column
    df_unemployment = df_unemployment.reset_index().drop('index', axis=1)

    # drop columns with data before TIME_HORIZON_START value
    df_unemployment = utilities.enforce_time_horizon_start(df_unemployment)

    # normalize state column values
    df_unemployment['state'] = df_unemployment['state'].replace(
        states_dict.states_dict)

    # remove state-level and nation-level records
    df_unemployment = df_unemployment[df_unemployment.county !=
                                      df_unemployment.state]
    df_unemployment = df_unemployment[df_unemployment.state != "US"]

    # normalize values for county column
    df_unemployment['county'] = df_unemployment['county'].str[:-4]
    df_unemployment['county'] = df_unemployment['county'].str.replace(
        ' County', '')

    # drop irrelevant columns
    df_unemployment.drop(['fips_txt', 'Rural_urban_continuum_code_2013',
                          'Urban_influence_code_2013', 'Metro_2013'], axis=1, inplace=True)
    # drop NaN rows
    df_unemployment.dropna(inplace=True)

    # casting and type checking
    for column in df_unemployment.columns:
        if column == 'state' or column == 'county':
            df_unemployment[column] = df_unemployment[column].astype(
                'string')
        elif 'rate' in column:
            df_unemployment[column] = df_unemployment[column].astype(
                'float')
        else:
            df_unemployment[column] = df_unemployment[column].astype(
                'int')
        print(str(column) + "    " + str(df_unemployment[column].dtype))
        assert(df_unemployment[column].dtype != 'object')

    # call utilities funtion to filter out non-states
    df_unemployment = utilities.remove_non_states(df_unemployment)

    df_unemployment.to_csv(
        '../../data/interim/UNEMPLOYMENT_by_COUNTY_2013_to_2019.csv', index=False)


if __name__ == '__main__':
    run()
