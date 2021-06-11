import pandas as pd
import glob

TIME_HORIZON_START = 2013


def remove_non_states(df):
    df_copy = df.copy()
    # Filter out US territories, protectorates, etc.
    for ele in ['AS', 'GU', 'MP', 'PR', 'VI',
                'Guam', 'Puerto Rico', "Virgin Islands"
                ]:
        df_copy = df_copy[df_copy.state != ele]
    return df_copy


def enforce_time_horizon_start(df):
    df_copy = df.copy()
    # drop columns with data before 2013
    for column_name in df_copy.columns:
        try:
            year = int(column_name[-4:])
            if year < TIME_HORIZON_START:
                df_copy.drop([column_name], axis=1, inplace=True)
        except Exception as e:
            print(e, column_name)
            continue
    return df_copy


def validate_indices():
    path = r'../../data/interim/'
    all_files = glob.glob(path + "/*.csv")
    for filename in all_files:
        print(f'Validation initiated for {filename}\n')
        df = pd.read_csv(filename, index_col=None, header=0)
        assert(len(df['state']) > 0)
        assert(0 == len([val for val in df['state'] if len(val) == 2]))
        assert(len(df['county']) > 0)
        assert(0 == len([val for val in df['county'] if 'County' in val]))
        assert(0 == len([val for val in df['county'] if ',' in val]))
        num_states = len(list(df.state.unique()))
        # Washington D.C. may not be counted in some datasets
        assert(num_states == 51 or num_states == 50)
        print(f'Index Validation complete for {filename}\n')

    print('Index Validation for all datasets complete')


INTERIM_DATA_PATH = f'../../data/interim/'
RAW_DATA_PATH = f'../../data/raw/'


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
