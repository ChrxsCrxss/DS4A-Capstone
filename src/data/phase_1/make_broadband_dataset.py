import json
import pandas as pd
import utilities


def run():
    path_to_raw_data = "../../data/raw/"

    df = pd.read_excel('../../data/raw/broadband_long2000-2018rev.xlsx')

    # filter out all rows that do not contain data. This operation only
    # leaves data from 2017 and 2018 for some counties. To keep the data
    # standardized for all counties, we drop all data for counties after
    # 2017 (inclusive)
    df_without_na = df[df['broadband'].notna()]
    df_broadband = df_without_na[df_without_na['year'] >= 2017]

    # rename columns
    df_broadband = df_broadband.rename(
        columns={'statenam': 'state', 'broadband': 'broadband percent'})

    # casting
    df_broadband['state'] = df_broadband['state'].astype('string')
    df_broadband['county'] = df_broadband['county'].astype('string')

    # drop unneeded columns
    df_broadband.drop(['id', 'cfips'], axis=1, inplace=True)

    # normalize values for county column
    df_broadband['county'] = df_broadband['county'].str.replace(' County', '')

    # type checking,
    for column in df_broadband.columns:
        print(str(column) + "    " + str(df_broadband[column].dtype))
        if column == 'state' or column == 'county':
            assert(df_broadband[column].dtype == 'string')
        else:
            assert(df_broadband[column].dtype != 'object')

    # call utilities funtion to filter out non-states
    df_broadband = utilities.remove_non_states(df_broadband)

    # save cleaned data to interm data directory
    df_broadband.to_csv(
        '../../data/interim/BROADBAND_2017_to_2018.csv', index=False)


if __name__ == '__main__':
    run()
