# National Data Opt Out publication

## Content 

This is the code for creating the [National Data Opt-out publication](https://digital.nhs.uk/data-and-information/publications/statistical/national-data-opt-out). 

This refactor of the previous code brings a number of improvements and benefits to the previous process: 
- removes a dependency on old csvs which store historic NDOP data from previous runs, enabling the process to produce all outputs for any period at any time.
- removes a dependency where list size publication must be produced and saved in a certain location in shared drive before NDOP can be run. It can now be run as soon as list size data is updated on SQL. 
- more controlled population of the Excel summary data, footnotes and headers using a tags identifier system. 
- clearer modularised code for easier development.   

## Outputs 

The script produces the following outputs for the publication: 
- An Excel summary, which presents the volumes of national data opt-outs over the latest 12 month period, with more detailed demographic breakdowns by age, gender and geography for the reporting month. 
- A csv containing National Data Opt-out volumns by age and gender over a 12 month period. 
- A csv containing National Data Opt-out counts by sub-ICB location of residence at Sub-ICB level over a 12 month period. 
- A csv containing National Data Opt-out counts by of registration at GP Practices at LSOA level over a 12 month period.   

## Pre-requisites 

- An IDE such as VScode. 
- Python 3.9 or above. 
- Approximately 9 GB of RAM available (this is quite a memory intensive process and may not be suitable to complete a full 12 month run when RDS is busy.)


## Initial setup 

Open VScode and run the code below to set up your virtual environment and install the necessary libraries. 

```python
python -m venv .venv
. .venv/Scripts/activate
pip install -r 'requirements.txt.
```
## Running the publication 

1. Activate your virtual environment using the following code: 
```python 
. .venv/Scripts/activate
```

2. Open a terminal and change your directory to the root of the folder. 
3. Run the publication using the following command
```python
python main.py --RPED <first-day-of-month> --pub_date <publication-date> 
```
- --RPED: This is the Report Period End Date and is usually the first day of the month of the most current data. This should be parsed as a string, in the format 'YYYY-MM-DD'
- --pub_date: The date the publication will be released. This should be parsed as a string, in the format 'YYYY-MM-DD' 

### Altering the publication reporting period 
By default, the publication will produce a time series that spans 12 months. To adjust the script to run for a shorter period time run the following command: 
```python
python main.py --RPED <first-day-of-month> --pub_date <publication-date> --months <number-of-months>
```

--months: The number of months that the publication should cover. 

## Support
If you have any questions about this repo, or suggestions on how we can improve the codebase, please get in touch here: gpdata.enquiries@nhs.net

## Directory structure 

This is the structure of the ndop module which handles the data preprocessing and Excel and csv production. 
```
📦ndop
 ┣ 📂config
 ┃ ┣ 📜config.py
 ┃ ┣ 📜params.py
 ┣ 📂csv
 ┃ ┣ 📜age_gen_csv.py
 ┃ ┣ 📜reg_geog_csv.py
 ┃ ┣ 📜res_geo_csv.py
 ┣ 📂excel
 ┃ ┣ 📜create_excel.py
 ┃ ┣ 📜excel_utils.py
 ┃ ┣ 📜footnotes.py
 ┃ ┣ 📜table_1.py
 ┃ ┣ 📜table_2.py
 ┃ ┣ 📜table_3.py
 ┃ ┗ 📜table_4.py
 ┣ 📂preprocessing
 ┃ ┣ 📜ingest_icb.py
 ┃ ┣ 📜list_size.py
 ┃ ┣ 📜lsoa.py
 ┃ ┣ 📜ndop_aggregate.py
 ┃ ┣ 📜ndop_clean.py
 ┗ 📜utils.py
```

## Modules

### config 

#### *config.config*

The config module contains functions involved with setup of the pipeline and reading in data from the SQL databases. It also contains a reportDate object which handles the majority of the date-related functionality for convenience. 

#### *config.params*
The parameters module holds static global variables used within the pipeline. It also contains addresses for the SQL databases. 

### csv 
The csv module contains the functions for creating the three CSV outputs for publication. The resulting dataframes that are produced from each of these modules are used to produce the 4 sheets of data in the Excel output. 

<n>N.B. In future, it might be ideal to separate production of the Excel data in a separate process to decouple it from the csv production process. 

#### *csv.age_gen_csv* 
This module produces the age_gen_csv which is the NDOP counts aggregated by age band and gender over the latest 12 months. As living and deceased patients are aggregated separately, the process is run with both dataframes for each of the patient types before concatenating the results. Patients who have an unknown gender or have preferred not to disclose their gender are not disaggregated by age. The data produced in this module is also used to produce sheet 2 of the Excel summary output.  

#### *csv.reg_geog_csv* 
This module produces the reg_geog_csv file which is the NDOP counts aggregated by the Sub-ICB location of the GP practice where the Opt-out is registered. Only active GP practices for each month will have Opt-outs counts recorded. Inactive practices, or practices which do not have a postcode will be classified and aggregated under 'Unallocated' practices. The data produced in this module is also used to produce sheet 3 and 4 of the Excel summary output.  

#### *csv.res_geog_csv* 
This module produces the res_geog_csv file which is the NDOP counts aggregated by the LSOA where the patient resides. The CSV also contains the Sub-ICB location of the LSOA associated with the record. Please note, even though a CCG of residence exists as a column in the NDOP database, records are not updated to reflect mapping changes so a separate mapping file is used for mapping the LSOA-Sub ICB locations.  Records with an invalid LSOA code are classified and aggregated under 'Unallocated'. 

### excel 
This module contains all the code associated with the production of the Excel publication summary. The module contains code from the Excel automation output repository that has useful helper functions to identify tags in the excel template. 

#### *excel.create_excel* 
This sub-module puts together the Excel summary by calling the sub-modules that produces each sheet and saves to outputs folder. 

#### *excel.excel_utils* 
This sub-module contains the helper functions from the Excel automation output repository. 

#### *excel.footnotes* 
*currently empty* This module intended use is to house all footnotes and their formatting values to easily alter and modify footnotes - room for development here in future! 

#### *excel.table sub-modules* 
A sub-module has been created for the production of each sheet in the Excel. Headers and Footnotes for each specific sheet are also defined separately in each module.  

### preprocessing
The preprocessing modules contains a number of functions that cleans and processes the raw data into a format that can be aggregated for the csv outputs and Excel tables. 

#### *preprocessing.ingest_icb* 
This is a back up for when ICB mappings changed and are not reflected in the reference tables. Contains functions for reading in mapping files in Excel format from inputs folder containing Sub-ICB - ICB - Region mappings. 

#### *preprocessing.list_size* 
This sub-module reads in list size data from SQL database and reshaped into a format that can be aggregated in later processes. Only practices which are active will have a list size number pulled through - they are filtered out in this sub-module.  

#### *preprocessing.lsoa* 
This is a back up for when ICB mappings changed and are not reflected in the reference tables. Contains functions for reading in mapping files in Excel format from inputs folder containing LSOA-Sub ICB mappings. 

#### *preprocessing.ndop_aggregate*
This submodule contains functions which aggregates the cleaned data into a number of dataframes for producing the outputs. Most notably, the concatenate_ndop_monthly_records function iterates through each month of the during the reporting period, returning active records for that month and concatenates those results into a large dataframe. This makes it easy to aggregate records over time. This process is repeated on active and deceased patients as these are counted separately. 

N.B. This probably the most memory-intensive process in the script as the concatenated dataframe can reach 40 million records over a 12 month period - perhaps this could be made more efficient?

#### *preprocessing.ndop_clean* 
This sub-module cleans the raw data by removing invalid NHS numbers, filling in null values in columns and remapping Gender and Age Band columns in preparation for aggregation. 

### utils 
The utils module contains a number of helper functions. 


## Useful links 
- [National Data Opt-out publication](https://digital.nhs.uk/data-and-information/publications/statistical/national-data-opt-out)
- [GPAD Outputs Automation repo](https://nhsd-git.digital.nhs.uk/data-services/analytics-service/primary-care/gpad-outputs-automation)

## License
The NDOP codebase is released under the MIT License.

The documentation is © Crown copyright and available under the terms of the Open Government 3.0 licence. https://www.nationalarchives.gov.uk/information-management/re-using-public-sector-information/uk-government-licensing-framework/crown-copyright/
