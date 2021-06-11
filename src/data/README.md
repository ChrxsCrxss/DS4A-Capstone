The purpose of this directory is to house scripts for running the ETL pipeline for project data. 

Extraction: 
	Most of the raw data is kept within the project in the data/raw folder. Geospatial data for 
	cryptocurrency-supporting venues is extracted via a single GET request from the Coinmap API. 

Transformation:
	Most data transformation occurs locally. The one exception of the data on venues: state and
	country info for EACH venue must be acquired via a GET request against the FCC Area API.
	This is a time-intensive operation that take upwards of 30 minutes.  

	For all datasets, the following transformation are performed in dataset-specific cleaning 
	scripts:

		1.) Type Casting
		2.) Column Filtering
		3.) Row Filtering 
		4.) Column Renaming
		4.) Column Name Normalization (All lowercase, no snake- or kabob- case) 
		... 

	
	INDEX NORMALIZATION is a crucial aspect of data transformation. All datasets must be indexable
	by state AND county before they can be loaded for EDA. The standard format for column naming
	for state and county columns will be as follows:
		
		df['state']
		df['county']

	The standard format for a state will be the unabbreviated, proper spelling of each state: 

		alabama  -> bad
		ALABAMA  -> bad
		AL       -> bad
		AL.      -> bad
		Alabama  -> good !

	The standard format for a county will be the unabbreviated, proper spelling of each county,
	without 'COUNTY' or the abbreviated state name afterwards:

		Cook County 	   -> bad
		Cook County, Ohio  -> bad
		Cook County, OH    -> bad
		Cook		   -> good !

	There will be a single script for index normalization that enforces this standard across all
	project datasets.

Loading:
	Data that has been properly filtered, cleaned, and normalized but NOT feature engineered will 
	be considered interim data and will be saved to data/interim. This will be the canonical data
	source for EDA. Datasets that have been feature engineered will be considered processed data
	and will be saved to data/processed. 
