import pandas as pd
import glob

import make_broadband_dataset
import make_coinmap_dataset
import make_poverty_dataset
import make_unemployment_dataset
import make_population_dataset


def etl_pipeline_init():
    print('Running ETL pipeline for all datasets in project')


def validate_indices():
    path = r'../../data/interim/'
    all_files = glob.glob(path + "/*.csv")
    for filename in all_files:
        try:
            df = pd.read_csv(filename, index_col=None, header=0)
            print(filename, df['state'].dtype)
            assert(len(df['state']) > 0)
            assert(0 == len([val for val in df['state'] if len(val) == 2]))
            assert(len(df['county']) > 0)
            assert(0 == len([val for val in df['county'] if 'County' in val]))
            assert(0 == len([val for val in df['county'] if ',' in val]))
            print(f'Index Validation complete for${filename}\n')
        except AssertionError as e:
            print(e)
            exit(1)

    print('Index Validation for all datasets complete')


if __name__ == '__main__':
    # service.py executed as script
    # do something
    etl_pipeline_init()
    make_broadband_dataset.run()
    make_coinmap_dataset.run()
    make_poverty_dataset.run()
    make_unemployment_dataset.run()
    make_population_dataset.run()
    validate_indices()
