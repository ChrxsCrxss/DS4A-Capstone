# Libraries needed for basic web-scraping
from IPython.core.display import HTML
from bs4 import BeautifulSoup, NavigableString, Tag
from IPython.display import IFrame
from urllib.request import urlopen, Request
import pandas as pd
import urllib
import os
import re
import traceback

# =================== getting coinatmradar data ================

# Scrape data from website
target_url = 'https://coinatmradar.com/country/226/bitcoin-atm-united-states/'
# fool the server :/
headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) '
           'AppleWebKit/537.11 (KHTML, like Gecko) '
           'Chrome/23.0.1271.64 Safari/537.11',
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
           'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
           'Accept-Encoding': 'none',
           'Accept-Language': 'en-US,en;q=0.8',
           'Connection': 'keep-alive'}

req = Request(url=target_url, headers=headers)

try:
    coinatmradar_usa_html = urlopen(req).read()
except Exception as e:
    print(e)
    exit(1)

# # Saving scraped HTML to .html file (for later processing)
# with open('coinatmradar_usa.html', 'w') as f:
#     f.write(coinatmradar_usa_html)

# Use html.parser to create soup
s = BeautifulSoup(coinatmradar_usa_html, 'html.parser')
# print(s.prettify())

cities = s.find_all('div', {'class': 'third'})
i = 0

city_venue_cnts = []
for column in cities:
    cur_state = ''
    for elem in column:
        if isinstance(elem, Tag):
            if 'class' in elem.attrs:
                cur_state = elem.text.split(',')[0]
                continue

            city_name = elem.a.text
            tmp_str_arr = elem.text.split(' ')
            venue_count = tmp_str_arr[len(tmp_str_arr)-1]
            try:
                venue_count = int(venue_count)
            except ValueError as e:
                print(e)
                continue

            city_venue_cnt = {}
            city_venue_cnt['city'] = city_name
            city_venue_cnt['state'] = cur_state
            city_venue_cnt['count'] = venue_count
            city_venue_cnts.append(city_venue_cnt)

# remove duplicate value for Birmingham
city_venue_cnts = city_venue_cnts[1:]
city_venue_cnts[0]['city'] = city_venue_cnts[0]['city'].split(',')[0]
df_venues_by_city = pd.json_normalize(city_venue_cnts)
# TODO: need to change this relative path when code is moved

INTERIM_DATA_TARGET = '../../data/interim/ATM_COUNT_BY_CTIY.csv'
df_venues_by_city.to_csv(INTERIM_DATA_TARGET, index=False)

print('Scraped ATM counts for {} cities in the United States'.format(
    df_venues_by_city.shape[0]))
# for cnt in city_venue_cnts:
#     print(cnt)


# =================== getting citydata data ================

df_venues_by_city = pd.read_csv('../../data/interim/ATM_COUNT_BY_CTIY.csv')
# this pipeline with extract a page from the city-data website
# for each city listed on coinatmradar.com. We will cash the
# pages in data/raw/city_data, since this extaction is so
# time-consuming


def strip_state_abbrv(broken_string):
    city = broken_string.split(',')
    city = list(filter(lambda x: len(x) > 3, city))
    return city[0]


city_state_path_dict = dict()
includes_state_abbrv = []

broken_urls = []
for index, row in df_venues_by_city.iterrows():
    try:
        city, state = '', ''

        if ',' in row['city']:
            city = strip_state_abbrv(row['city'])
        else:
            city = row['city']

        glue = '-'
        city = str(city).split(' ')
        city = glue.join(city)
        state = str(row['state']).split(' ')
        state = glue.join(state)

        # missing data for some Indiana cities
        city_data_url = f'https://www.city-data.com/city/{city}-{state}.html'
        # print(city_data_url)

        file_path = f'../../data/raw/city_data/city-data-{city}-{state}.html'
        if os.path.isfile(file_path):
            city_state_path_dict[f'city-data-{city}-{state}.html'] = {
                'city': city, 'state': state}
            #print('local copy of html page detected')
            continue

        r = urllib.request.urlopen(city_data_url)

        site_content = r.read().decode('utf-8')

        # Saving scraped HTML to .html file (for later processing)
        with open(file_path, 'w') as f:
            f.write(site_content)

        # create mapping between filename and city and state index
        city_state_path_dict[f'city-data-{city}-{state}.html'] = {
            'city': city, 'state': state}

    except urllib.error.HTTPError as err:
        print(err)
        print('city:', city, 'state:', state)
        broken_urls.append(city_data_url)
        continue
    except AttributeError as err:
        print(err)
        broken_urls.append(city_data_url)
        continue


file_arr = [f for f in os.listdir('../data/raw/city_data/')]

# This is a dictionary of arrays that will hold dictionaries. Each value
# in df_dict represents a future pandas dataframe
df_dict = {
    "general_demographic_data": [],
    "race_data": [],
    "industry_data": [],
    "occupation_data": [],
    "food_env_data": [],
    "expenditure_data": [],
    "debt_data": [],
    "assets_data": [],
    "revenue_data": [],
    "to_state_comparisons_data": []
}

file_arr = [f for f in os.listdir('../../data/raw/city_data/')]

for file in file_arr:
    print(file, city_state_path_dict[file])

    html = ''
    with open(f'../../data/raw/city_data/{file}', 'r') as f:
        html = f.read()

    s = BeautifulSoup(html, 'html.parser')
    all_data_for_city = html_extractor.get_all_data(
        s, df_index=city_state_path_dict[file])

    # All each dictionary (soon-to-be-row) to its respective array (soon-to-be-column).
    # Basically, we move dicts from a dict of dicts to a dict of lists, each of which
    # contains dicts
    for key in all_data_for_city:
        df_dict_idx = key[:-4]
        df_dict[df_dict_idx].append(all_data_for_city[key])

# After we scraping each html page, use list comphrehension to
# filter out None values in each arrary of dicts,
# then create the dataframe and save it to 'data/interim'
for key in df_dict:
    df_dict[key] = [x for x in df_dict[key] if x is not None]

    if key == "to_state_comparisons_data":
        df_dict[key] = fix_comparison_data(df_dict[key])

    new_df = pd.json_normalize(df_dict[key])
    new_df.to_csv(f'../../data/interim/{key}.csv', index=False)

    print(
        'New DataFrame created. Relative path is:  ../../data/interim/{key}.csv')
    print('At a glance:\n')
    print('Shape: ', new_df.shape)
    print('Columns: ', new_df.dtypes)


def align_compare_data(row, prefix, columns):
    for column in columns:
        # print(row[column])
        if not pd.isna(row[column]):
            if prefix in row[column]:
                return row[column].replace(prefix, '')


# Some of the data is returned in very clumbersone formats. To
# produce usable datasets, we have to apply a lambda function
# against to produce a new columns with all values propertly
# aligned. This function basically uses the lambda function
# align_compare_data to make sure categorical values for the
# same variable are all in the same column. It then returns
# a dataframe with just these new columns (names in prefix_arr)
def fix_comparison_data(compare_arr):
    prefix_arr = [
        "Median household income",
        "Median house value",
        "Unemployed percentage",
        "Black race population percentage",
        "Hispanic race population percentage",
        "Median age",
        "Foreign-born population percentage",
        "House age",
        "Number of college students",
        "Renting percentage",
    ]

    compare_df = pd.json_normalize(compare_arr)
    columns = compare_df.columns
    for prefix in prefix_arr:
        compare_df[prefix] = compare_df.apply(
            lambda row: align_compare_data(row, prefix, columns), axis=1)

    return compare_df[prefix_arr]
