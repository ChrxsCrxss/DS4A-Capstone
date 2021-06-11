import pandas as pd
import utilities
import states_dictionary


def run():
    df_edu = pd.read_excel('../../data/raw/Education.xls')

    # rename columns to value of row with index of 3
    df_edu = df_edu.rename(columns=df_edu.iloc[3])

    # drop first four columns (excel formatting)
    df_edu = df_edu.drop([0, 1, 2, 3])
    # drop all rows with NA
    df_edu.dropna(inplace=True)

    # drop columns with data before TIME_HORIZON_START value
    df_edu = utilities.enforce_time_horizon_start(df_edu)

    df_edu.drop(['FIPS Code', '2003 Rural-urban Continuum Code', '2003 Urban Influence Code',
                 '2013 Rural-urban Continuum Code', '2013 Urban Influence Code'], axis=1, inplace=True)

    df_edu = df_edu.rename(columns={'State': 'state', 'Area name': 'county'})

    # normalize state column values
    df_edu['state'] = df_edu['state'].replace(states_dictionary.states_dict)

    # normalize values for county column
    df_edu['county'] = df_edu['county'].str.replace(' County', '')

    # casting and type checking
    for column in df_edu.columns:
        if column == 'state' or column == 'county':
            df_edu[column] = df_edu[column].astype(
                'string')
        elif 'Percent' in column:
            df_edu[column] = df_edu[column].astype(
                'float')
        else:
            df_edu[column] = df_edu[column].astype(
                'int')
        print(str(column) + "    " + str(df_edu[column].dtype))
        assert(df_edu[column].dtype != 'object')

    df_edu = utilities.remove_non_states(df_edu)
    df_edu.to_csv('../../data/interim/EDUCATION.csv', index=False)
