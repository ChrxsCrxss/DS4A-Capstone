import json
import pandas as pd
import states_dictionary as states_dict


def run():
    df_poverty = pd.read_excel('../../data/raw/PovertyEstimates.xls')

    # Rename columns to the fields in row 3 then rename two of those columns to State and State_Couny
    df_poverty = df_poverty.rename(columns=df_poverty.iloc[3])
    df_poverty = df_poverty.rename(
        columns={'Stabr': 'state', 'Area_name': 'county'})

    # drop the first four row (poor excel formatting)
    df_poverty = df_poverty[4:]

    # drop columns with data before 2013
    for column_name in df_poverty.columns:
        try:
            year = int(column_name[-4:])
            if year < 2013:
                df_poverty.drop([column_name], axis=1, inplace=True)
        except Exception as e:
            print(e, column_name)
            continue

    # remove confidence interval columns and other unneeded column
    df_poverty = df_poverty[["state", "county", "POVALL_2019", "PCTPOVALL_2019", "POV017_2019", "PCTPOV017_2019",
                            "MEDHHINC_2019",
                             ]]

    # rename columns using dictionary
    column_name_dict = {
        "POVALL_2019": "total_poverty_2019",
        "PCTPOVALL_2019": "poverty_percent_all_2019",
        "POV017_2019": "total_childhood_people_2019",
        "PCTPOV017_2019": "childhood_poverty_percent_all_2019",
        "MEDHHINC_2019": "median_household_income_2019",
    }
    df_poverty = df_poverty.rename(columns=column_name_dict)

    for column in df_poverty.columns:
        print(column)

    # normalize state column values
    df_poverty['state'] = df_poverty['state'].replace(states_dict.states_dict)

    # normalize values for county column
    df_poverty['county'] = df_poverty['county'].str.replace(' County', '')

    # remove state-level and nation-level records
    df_poverty = df_poverty[df_poverty.county != df_poverty.state]
    df_poverty = df_poverty[df_poverty.state != "US"]

    # cast columns to appropriate data types
    for column in df_poverty.columns:
        if column in ["state", "county"]:
            df_poverty[column] = df_poverty[column].astype('string')
        else:
            df_poverty[column] = df_poverty[column].astype('float')

        # type checking
        print(str(column) + "    " + str(df_poverty[column].dtype))
        assert(df_poverty[column].dtype != 'object')

    df_poverty.to_csv(
        '../../data/interim/POVERTY_ESTIMATES_2019.csv', index=False)


if __name__ == '__main__':
    run()
