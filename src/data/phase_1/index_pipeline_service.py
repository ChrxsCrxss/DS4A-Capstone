import make_broadband_dataset
import make_coinmap_dataset
import make_poverty_dataset
import make_unemployment_dataset
import make_population_dataset
import make_urbanRuralContCodes_dataset
import make_education_dataset
import utilities


def etl_pipeline_init():
    print('Running ETL pipeline for all datasets in project')


if __name__ == '__main__':
    # service.py executed as script
    # do something
    etl_pipeline_init()
    make_broadband_dataset.run()
    make_coinmap_dataset.run()
    make_poverty_dataset.run()
    make_unemployment_dataset.run()
    make_population_dataset.run()
    make_urbanRuralContCodes_dataset.run()
    make_education_dataset.run()
    utilities.validate_indices()
