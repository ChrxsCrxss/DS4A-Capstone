import json
import pandas as pd
import utilities


def run():
    df_pop = pd.read_excel('../../data/raw/PopulationEstimates.xls')

    # Rename columns to the fieds in row with index 1
    df_pop = df_pop.rename(columns=df_pop.iloc[1])

    # drop the first 4 rows that contain all Nan values and/or column headers
    df_pop = df_pop.drop([0, 1, 2, 3])

    # drop columns with data before TIME_HORIZON_START value
    df_pop = utilities.enforce_time_horizon_start(df_pop)

    # this will drop state-level data and data for Puerto Rico
    df_pop = df_pop.dropna()

    # normalize state and county column names
    df_pop = df_pop.rename(columns={'State': 'state', 'Area_Name': 'county'})

    # drop irrelevant columns
    df_pop.drop(['Rural-urban_Continuum Code_2013', 'Urban_Influence_Code_2013',
                 'Economic_typology_2015', 'FIPStxt'], axis=1, inplace=True)

    # normalize state column values
    states_dict = {"AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "DC": "Washington D.C.", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
                   "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming"}
    df_pop['state'] = df_pop['state'].replace(states_dict)

    # normalize values for county column
    df_pop['county'] = df_pop['county'].str.replace(' County', '')

    # casting and type checking
    for column in df_pop.columns:
        if column == 'state' or column == 'county':
            df_pop[column] = df_pop[column].astype('string')
        else:
            df_pop[column] = df_pop[column].astype('float')
        print(str(column) + "    " + str(df_pop[column].dtype))
        assert(df_pop[column].dtype != 'object')

    # call utilities funtion to filter out non-states
    df_pop = utilities.remove_non_states(df_pop)

    df_pop.to_csv(
        '../../data/interim/POPULATION_ESTIMATES_2013_to_2019.csv', index=False)


if __name__ == '__main__':
    run()
