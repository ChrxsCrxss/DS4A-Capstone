import json
import pandas as pd
import numpy as np
import utilities

"""
Rural-Urban Continuum Codes
The 2013 Rural-Urban Continuum Codes form a classification scheme 
that distinguishes metropolitan counties by the population size of 
their metro area, and nonmetropolitan counties by degree of 
urbanization and adjacency to a metro area. The official Office of 
Management and Budget (OMB) metro and nonmetro categories have been 
subdivided into three metro and six nonmetro categories. Each county 
in the U.S., municipio in Puerto Rico, and Census Bureau-designated 
county-equivalent area of the Virgin Islands/other inhabited island 
territories of the U.S. is assigned one of the 9 codes. This scheme 
allows researchers to break county data into finer residential groups,
beyond metro and nonmetro, particularly for the analysis of trends in 
nonmetro areas that are related to population density and metro 
influence. The Rural-Urban Continuum Codes were originally developed 
in 1974. They have been updated each decennial since 
(1983, 1993, 2003, 2013), and slightly revised in 1988. Note that the 
2013 Rural-Urban Continuum Codes are not directly comparable with the 
codes prior to 2000 because of the new methodology used in developing 
the 2000 metropolitan areas. See the Documentation for details and a 
map of the codes.

An update of the Rural-Urban Continuum Codes is planned for mid-2023.

See: 
https://www.ers.usda.gov/data-products/rural-urban-continuum-codes.aspx 
"""


def run():
    df_urcc = pd.read_excel('../../data/raw/ruralurbancodes2013.xls')

    df_urcc.drop(['FIPS', 'Population_2010'], axis=1, inplace=True)
    df_urcc = df_urcc.dropna()
    df_urcc['RUCC_2013'] = df_urcc['RUCC_2013'].astype('int')

    df_urcc = df_urcc.rename(
        columns={"State": "state", "County_Name": "county"})

    # normalize state column values
    states_dict = {"AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "DC": "Washington D.C.", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
                   "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming"}
    df_urcc['state'] = df_urcc['state'].replace(states_dict)

    # normalize values for county column
    df_urcc['county'] = df_urcc['county'].str.replace(' County', '')

    # remove census tract data as well. LITERALLY ONE value
    # - `Hoonah-Angoon Census Area, AK` - caused the pipeline
    # to break
    for cnty in df_urcc['county']:
        if 'Census Area' in cnty:
            df_urcc = df_urcc[df_urcc['county'] != cnty]

    # call utilities funtion to filter out non-states
    df_urcc = utilities.remove_non_states(df_urcc)

    df_urcc = df_urcc.to_csv(
        '../../data/interim/RURAL_URBAN_CODES_2013.csv', index=False)


if __name__ == '__main__':
    run()
