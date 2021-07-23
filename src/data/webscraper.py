# Libraries needed for basic web-scraping
from IPython.core.display import HTML
from bs4 import BeautifulSoup, NavigableString, Tag
import traceback
import re


def containsDigit(variable):
    return any(char.isdigit() for char in variable)


def containsMoneySign(variable):
    return '$' in variable


def stripNonNumeric(variable):
    return re.sub("[^0-9^.]", "", variable)


def stripNonNumericAndDecimal(variable):
    return re.sub("[^0-9]", "", variable)


class city_data_html_extractor():
    def __init__(self):
        self.general_demographic_data_all = []
        self.racial_data_all = []
        self.industries_and_occupations_data_all = []
        self.food_env_data_all = []
        self.extraction_errors = 0

    def exception_handler(self, e):
        print(e)
        self.extraction_errors += 1

    @staticmethod
    def containsDigit(variable):
        return any(char.isdigit() for char in variable)

    @staticmethod
    def containsMoneySign(variable):
        return '$' in variable

    @staticmethod
    def stripNonNumeric(variable):
        return re.sub("[^0-9^.]", "", variable)

    @staticmethod
    def stripNonNumericAndDecimal(variable):
        return re.sub("[^0-9]", "", variable)

    def get_all_data(self, s, df_index):
        all_data_dict = {
            'general_demographic_data_row': self.get_general_demographic_data(s, df_index),
            'race_data_row': self.get_racial_data(s, df_index),
            'industry_data_row': self.get_industries_and_occupations_data(s, df_index)['industry_dict'],
            'occupation_data_row': self.get_industries_and_occupations_data(s, df_index)['occupation_dict'],
            'food_env_data_row': self.get_food_env_data(s, df_index),
            'expenditure_data_row': self.get_city_expenditure_data(s, df_index),
            'debt_data_row': self.get_city_debt_data(s, df_index),
            'assets_data_row': self.get_city_assets_data(s, df_index),
            'revenue_data_row': self.get_city_revenue_dict(s, df_index),
            'to_state_comparisons': self.get_to_state_comparision_data(s, df_index)
        }
        return all_data_dict

    def get_general_demographic_data(self, s, df_index):
        city_general_demographic_data = dict(df_index)
        city_general_demographic_data.update(
            self.get_population_data(s, df_index) or {})
        city_general_demographic_data.update(
            self.get_median_age_data(s, df_index) or {})
        city_general_demographic_data.update(
            self.get_median_income_data(s, df_index) or {})
        city_general_demographic_data.update(
            self.get_median_rent_data(s, df_index) or {})
        city_general_demographic_data.update(
            self.get_cost_of_living_data(s, df_index) or {})
        city_general_demographic_data.update(
            self.get_pop_density_data(s, df_index) or {})
        return city_general_demographic_data

    def get_population_data(self, s, df_index):
        try:
            city_population_data = s.find('section', {'id': 'city-population'})
            city_general_demographic_data = dict(df_index)
            for elem in city_population_data:
                if isinstance(elem, NavigableString):
                    pop_data_arr = elem.split(' ')
                    pop_data_arr = list(filter(containsDigit, pop_data_arr))
                    try:
                        pop_data_arr = list(map(stripNonNumeric, pop_data_arr))
                        city_general_demographic_data['total population 2019'] = int(
                            stripNonNumericAndDecimal(pop_data_arr[0]))
                        if (len(pop_data_arr) >= 3):
                            city_general_demographic_data['percent urban 2019'] = pop_data_arr[1]
                            city_general_demographic_data['percent rural 2019'] = pop_data_arr[2]
                        return city_general_demographic_data
                    except IndexError as e:
                        print('<get_population_data>', e)
                        print(
                            'could not locate all data for population for', df_index['city'])
                        self.extraction_errors += 1
                        return city_general_demographic_data
        except Exception as e:
            print(e)
            print(traceback.format_exc())
            self.extraction_errors += 1

    def get_median_age_data(self, s, df_index):
        median_age_data = s.find('section', {'id': 'median-age'})
        try:
            city_general_demographic_data = dict(df_index)
            for elem in median_age_data:
                idx = elem.text.find(':')
                # median age is like 34.4, for example
                median_resident_age = elem.text[idx+1:idx+5]
                city_general_demographic_data['median age'] = float(
                    median_resident_age)
            return city_general_demographic_data
        except Exception as e:
            print('<get_median_age_data>', e)
            print(traceback.format_exc())
            print(median_age_data)
            print(df_index)
            self.extraction_errors += 1

    def get_median_income_data(self, s, df_index):
        try:
            median_income_data = s.find('section', {'id': 'median-income'})
            city_general_demographic_data = dict(df_index)
            for elem in median_income_data:
                if isinstance(elem, Tag):
                    if 'Estimated per capita income' in elem.text:
                        income_arr = list(
                            filter(containsDigit, elem.text.splitlines()))
                        for elem in income_arr:
                            if 'Estimated per capita income in 2019' in elem:
                                temp_str = elem.split(':')[1]
                                temp_str_arr = temp_str.split('(')
                                temp_str = temp_str_arr[0]
                                median_income = stripNonNumeric(temp_str)
                                city_general_demographic_data['median income 2019'] = float(
                                    median_income)
                            if 'Estimated median house or condo value in 2019' in elem:
                                temp_str = elem.split(':')[1]
                                temp_str_arr = temp_str.split('(')
                                temp_str = temp_str_arr[0]
                                median_house_price = stripNonNumeric(temp_str)
                                city_general_demographic_data['median house price 2019'] = float(
                                    median_house_price)
            return city_general_demographic_data
        except Exception as e:
            print('<get_median_income_data>', e)
            self.extraction_errors += 1

    def get_median_rent_data(self, s, df_index):
        try:
            median_rent_data = s.find('section', {'id': 'median-rent'})
            city_general_demographic_data = dict(df_index)
            for elem in median_rent_data:
                if isinstance(elem, Tag):
                    # print('elem:',elem.text)
                    median_rent_arr = elem.text.split(':')
                    median_rent = int(
                        stripNonNumericAndDecimal(median_rent_arr[1]))
                    city_general_demographic_data['median rent 2019'] = float(
                        median_rent)
            return city_general_demographic_data
        except Exception as e:
            print('<get_median_rent_data>', e)
            self.extraction_errors += 1

    def get_cost_of_living_data(self, s, df_index):
        try:
            cost_of_living_index_data = s.find(
                'section', {'id': 'cost-of-living-index'})
            city_general_demographic_data = dict(df_index)
            for elem in cost_of_living_index_data:
                if isinstance(elem, NavigableString):
                    cost_of_living_index = float(elem)
                    city_general_demographic_data['cost of living index'] = float(
                        cost_of_living_index)
            return city_general_demographic_data
        except Exception as e:
            print('<get_cost_of_living_data>', e)
            self.extraction_errors += 1

    def get_pop_density_data(self, s, df_index):
        try:
            population_density_data = s.find(
                'section', {'id': 'population-density'})
            city_general_demographic_data = dict(df_index)
            for elem in population_density_data:
                if isinstance(elem, Tag):
                    if 'Population density' in elem.text:
                        pop_density = stripNonNumericAndDecimal(elem.text)
                        city_general_demographic_data['population density 2019'] = float(
                            pop_density)
            return city_general_demographic_data
        except Exception as e:
            print('<get_pop_density_data>', e)
            self.extraction_errors += 1

    def get_racial_data(self, s, df_index):
        try:
            city_racial_data = dict(df_index)
            racial_brkdwn_data = s.find('section', {'id': 'races-graph'})
            for elem in racial_brkdwn_data:
                if isinstance(elem, Tag):
                    cracial_brkdwn_data_arr = str.splitlines(elem.text)
                    cracial_brkdwn_data_arr = list(
                        filter(stripNonNumeric, cracial_brkdwn_data_arr))
                    for elem in cracial_brkdwn_data_arr:
                        elem_arr = elem.split('%')
                        data = elem_arr[0]  # numeric values
                        key = elem_arr[1]
                        idx = elem.rfind(',')

                        # This is true when the total count is atleast 1000
                        if idx != -1:
                            city_racial_data[key] = float(data[idx+4:])
                        # This is true when the total count is less than 1000
                        else:
                            city_racial_data[key] = float(data[-3:])
            return city_racial_data
        except Exception as e:
            print('<get_racial_data>', e)
            self.extraction_errors += 1

    def get_industries_and_occupations_data(self, s, df_index):
        try:
            industries_and_occupations_data = s.find(
                'section', {'id': 'most-common-industries'})
            industry_dict = dict(df_index)
            occupation_dict = dict(df_index)
            for elem in industries_and_occupations_data:
                if isinstance(elem, Tag):
                    industries_and_occupation_arr_raw = str.splitlines(
                        elem.text)
                    # Apply lambda function to return only elems with a positive length, then caset to list
                    industries_and_occupation_arr = list(
                        filter(lambda x: len(x) > 0, industries_and_occupation_arr_raw))
                    industries_arr = []
                    occupation_arr = []
                    # Array contains information on both industries and occupations, so we'll
                    # need to splice at a convienent index... html hell :(
                    for idx, elem in enumerate(industries_and_occupation_arr):
                        if 'Most common occupations' in elem:
                            industries_arr = industries_and_occupation_arr[0:idx]
                            occupation_arr = industries_and_occupation_arr[idx:]
                            break
                    #print(industries_and_occupation_arr, "\n\n")

                    # We don't want elements that don't contain numeric values
                    industries_arr = list(filter(lambda x: any(
                        char.isdigit() for char in x), industries_arr))
                    occupation_arr = list(filter(lambda x: any(
                        char.isdigit() for char in x), occupation_arr))
                    # print(industries_arr)
                    # print(occupation_arr)

                    for elem in industries_arr:
                        elem_str_arr = elem.split('(')
                        # print(elem_str_arr)
                        category = elem_str_arr[0][:len(elem_str_arr[0])-1]
                        value = elem_str_arr[1].replace(
                            '%', '').replace(')', '')
                        # print(value)
                        # print(category)
                        industry_dict[category.lower()] = float(value)

                    for elem in occupation_arr:
                        elem_str_arr = elem.split('(')
                        # print(elem_str_arr)
                        category = elem_str_arr[0][:len(elem_str_arr[0])-1]
                        value = elem_str_arr[1].replace(
                            '%', '').replace(')', '')
                        # print(value)
                        # print(category)
                        occupation_dict[category.lower()] = float(value)
            return {'industry_dict': industry_dict, 'occupation_dict': occupation_dict}
        except Exception as e:
            print('<get_industries_and_occupations_data>', e)
            self.extraction_errors += 1

    def get_food_env_data(self, s, df_index):
        try:
            food_environment_data = s.find(
                'section', {'id': 'food-environment'})
            food_env_dict = dict(df_index)
            for elem in food_environment_data:
                if isinstance(elem, Tag):
                    food_env_data_str = str(elem.text)
                    if 'Number of' in food_env_data_str:
                        category = food_env_data_str.split(
                            ':')[0].replace('Number of', '')

                        # Basically we need to try a number of different strings to
                        # begin the index, since webpages data is perfected normalized
                        idx = food_env_data_str.lower().find('County:'.lower())
                        if idx != -1:
                            start_idx = idx + len('County:')
                            value = food_env_data_str[start_idx:start_idx+4]
                            food_env_dict[category[1:] +
                                          ' per 10k'] = float(value)
                            continue

                        idx = food_env_data_str.lower().find('Here:'.lower())
                        if idx != -1:
                            start_idx = idx + len('Here:')
                            value = food_env_data_str[start_idx:start_idx+4]
                            food_env_dict[category[1:] +
                                          ' per 10k'] = float(value)
                            continue

                        cur_city = df_index['city']
                        city_string = f'{cur_city} city'
                        idx = food_env_data_str.lower().find(city_string.lower())
                        if idx != -1:
                            start_idx = idx + len(city_string)
                            value = food_env_data_str[start_idx:start_idx+4]
                            food_env_dict[category[1:] +
                                          ' per 10k'] = float(value)
                            continue

                        print('Did not find target string "County:" in string...')
                        print(food_env_data_str, "\n\n")
                    else:
                        continue
            return food_env_dict
        except Exception as e:
            print('<get_food_env_data>', e)
            self.extraction_errors += 1

    def get_city_expenditure_data(self, s, df_index):
        try:
            govFinances_expenditure_data = s.find(
                'div', {'id': 'govFinancesE'})
            fin_expenditure_dict = dict(df_index)
            for elem in govFinances_expenditure_data:
                if isinstance(elem, Tag):
                    # print('elem:',elem.text)
                    revenue_data_arr = str.splitlines(elem.text)
                    revenue_data_arr = list(
                        filter(containsDigit, revenue_data_arr))
                    revenue_data_arr = list(
                        filter(containsMoneySign, revenue_data_arr))
                    for elem in revenue_data_arr:
                        key = elem.split(':')[0]
                        value = elem.split(':')[1]
                        value = value.split('(')[1].split(')')[0]
                        value = stripNonNumeric(value)
                        fin_expenditure_dict[key] = value
            return fin_expenditure_dict
        except Exception as e:
            print('<get_city_expenditure_data>', e)
            self.extraction_errors += 1

    def get_city_debt_data(self, s, df_index):
        try:
            govFinances_debt_data = s.find('div', {'id': 'govFinancesD'})
            fin_debt_dict = dict(df_index)
            for elem in govFinances_debt_data:
                if isinstance(elem, Tag):
                    # print('elem:',elem.text)
                    debt_data_arr = str.splitlines(elem.text)
                    debt_data_arr = list(filter(containsDigit, debt_data_arr))
                    debt_data_arr = list(
                        filter(containsMoneySign, debt_data_arr))
                    for elem in debt_data_arr:
                        key = elem.split(':')[0]
                        value = elem.split(':')[1]
                        value = value.split('(')[1].split(')')[0]
                        value = stripNonNumeric(value)
                        fin_debt_dict[key] = value
            return fin_debt_dict
        except Exception as e:
            print('<get_city_debt_data>', e)
            self.extraction_errors += 1

    def get_city_assets_data(self, s, df_index):
        try:
            govFinances_assets_data = s.find('div', {'id': 'govFinancesC'})
            fin_assets_dict = dict(df_index)
            for elem in govFinances_assets_data:
                if isinstance(elem, Tag):
                    # print('elem:',elem.text)
                    assets_data_arr = str.splitlines(elem.text)
                    assets_data_arr = list(
                        filter(containsDigit, assets_data_arr))
                    assets_data_arr = list(
                        filter(containsMoneySign, assets_data_arr))
                    for elem in assets_data_arr:
                        key = elem.split(':')[0]
                        value = elem.split(':')[1]
                        value = value.split('(')[1].split(')')[0]
                        value = stripNonNumeric(value)
                        fin_assets_dict[key] = value
            return fin_assets_dict
        except Exception as e:
            print('<get_city_assets_data>', e)
            self.extraction_errors += 1

    def get_city_revenue_dict(self, s, df_index):
        try:
            govFinances_revenue_data = s.find('div', {'id': 'govFinancesR'})
            fin_revenue_dict = dict(df_index)
            for elem in govFinances_revenue_data:
                if isinstance(elem, Tag):
                    # print('elem:',elem.text)
                    revenue_data_arr = str.splitlines(elem.text)
                    revenue_data_arr = list(
                        filter(containsDigit, revenue_data_arr))
                    revenue_data_arr = list(
                        filter(containsMoneySign, revenue_data_arr))
                    for elem in revenue_data_arr:
                        key = elem.split(':')[0]
                        value = elem.split(':')[1]
                        value = value.split('(')[1].split(')')[0]
                        value = stripNonNumeric(value)
                        fin_revenue_dict[key] = value
            return fin_revenue_dict
        except Exception as e:
            print('<get_city_revenue_dict>', e)
            self.extraction_errors += 1

    def get_to_state_comparision_data(s, df_index):
        to_state_comparisons = s.find('section', {'id': 'comparisons'})
        for elem in to_state_comparisons:
            if isinstance(elem, Tag):
                compare_data_arr = str.splitlines(elem.text)
                if len(compare_data_arr) > 1:
                    compare_data_arr = list(
                        filter(lambda x: len(x) > 0, compare_data_arr))
                    print(compare_data_arr, "\n\n")

                    compare_dict = dict(df_index)
                    # to make it more dynamic?
                    for idx, val in enumerate(compare_data_arr):
                        compare_dict['Row {}'.format(idx)] = val
                    return compare_dict
