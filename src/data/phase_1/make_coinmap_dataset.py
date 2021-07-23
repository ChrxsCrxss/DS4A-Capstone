import json
import pandas as pd
import requests
import os.path
import utilities

"""
This is the main portion of the ETL pipeline for coinmap data. We make
a call against the FCC Area API to obtain state and county data. This 
process also filters out all venues not located in the United States. 
This is a time-intensive operation, so we cache the result in data/raw
as soon as possible 
"""


def run():
    if not os.path.isfile('../../data/raw/COINMAP_DATA_USA'):
        print('Local data cache not found. Fetching data from remote sources...\n')
        response = requests.get('https://coinmap.org/api/v1/venues/')
        data = response.json()
        venues_array = data['venues']

        # simple checks that we actually got data
        assert(venues_array != None)
        assert(len(venues_array) > 0)

        print(f"{utilities.bcolors.OKGREEN}GET request from Coinmap API successful. Getting state and county info...\n{utilities.bcolors.ENDC}")

        # This filtering step reduces the number of records from 22787 to 7379
        # This still means we have to call the API 7379 times. Unfortunately
        # there are no further simple optimization we can make, since it turns
        # out that >6000 of the 7379 actually lie within the United States
        southernmost_lat = 18.464825  # southern tip of Hawaii
        easternmost_lon = -66.949471  # eastern tip of Maine
        venues_array = list(filter(lambda ven: int(
            ven['lon']) <= easternmost_lon, venues_array))
        venues_array = list(filter(lambda ven: int(
            ven['lat']) >= southernmost_lat, venues_array))

        print(str(len(list(venues_array))) + " records obtained")

        # this array will contain all venues in the United States once
        # the data is filtered against the FCC Area API
        venues_usa = []

        for venue in venues_array:

            lat_param = venue['lat']
            lon_param = venue['lon']

            # Get response and convert it into a json
            response = requests.get(
                f'https://geo.fcc.gov/api/census/block/find?latitude={lat_param}&longitude={lon_param}&format=json')

            if response.status_code == 200:
                data = response.json()

                # We cast the state name and county name to string becuase when the coordinates lie
                # outside of the United States, these values are returned as type <class 'NoneType'>.
                # In this case, we simply skip this record and move on to the next on in the veneus_array.
                if str(data['State']['name']) == 'None' or str(data['County']['name']) == 'None':
                    continue

                # If the venue is in the United States, we assign the state name and country name to the
                # current venue record. We then append this record, which now contains fields for state and
                # county to the venues_usa list. Casting to a string here is a defensive operation.
                else:
                    venue['state'] = str(data['State']['name'])
                    venue['county'] = str(data['County']['name'])
                    venues_usa.append(venue)
            else:
                print(response.status_code, response)

        print(f"{utilities.bcolors.OKGREEN}Filtering complete. Saving data to ../../data/raw/COINMAP_DATA_USA...\n{utilities.bcolors.ENDC}")
        # Since the continuous process of calling the API for each record is so expensive
        # we will want to save the data as soon as possible. Although the data has been
        # feature enigeered to some degree, it is still 'raw', so we will save it to the
        # raw directory immediately
        df_venues_usa = pd.json_normalize(venues_usa)
        df_venues_usa.to_csv('../../data/raw/COINMAP_DATA_USA', index=False)

    else:
        print(f'{utilities.bcolors.OKBLUE}Local data cache detected. Reading from ../../data/raw/COINMAP_DATA_USA...\n{utilities.bcolors.ENDC}')

    df_venues_usa = pd.read_csv('../../data/raw/COINMAP_DATA_USA')

    # Convert seconds since epoch to readable datetime
    df_venues_usa['created_on'] = pd.to_datetime(
        df_venues_usa['created_on'], unit='s')
    df_venues_usa['year'] = df_venues_usa['created_on'].dt.year

    # drop irrelevant columns
    df_venues_usa.drop(['id', 'promoted', 'name', 'created_on',
                        'geolocation_degrees'], axis=1, inplace=True)

    # reorder columns so indices are to the left
    df_venues_usa = df_venues_usa[[
        'state', 'county', 'lat', 'lon', 'year', 'category']]

    # casting
    df_venues_usa['state'] = df_venues_usa['state'].astype('string')
    df_venues_usa['county'] = df_venues_usa['county'].astype('string')
    df_venues_usa['category'] = df_venues_usa['category'].astype('string')

    # type checking,
    for column in df_venues_usa.columns:
        print(str(column) + "    " + str(df_venues_usa[column].dtype))
        if column == 'state' or column == 'county':
            assert(df_venues_usa[column].dtype == 'string')
        else:
            assert(df_venues_usa[column].dtype != 'object')

    # call utilities funtion to filter out non-states
    df_venues_usa = utilities.remove_non_states(df_venues_usa)

    # save to interm data folder
    print(f"{utilities.bcolors.OKGREEN}Coinmap data extraction and transformation complete. Saving to ../../data/interim/CRYTO_VENUES_USA...\n{utilities.bcolors.ENDC}")
    df_venues_usa.to_csv(
        '../../data/interim/CRYTO_VENUES_USA.csv', index=False)


if __name__ == '__main__':
    run()
