# tableau-metadata-utility

This utility gets all Tableau metadata querying the Graph API for workbooks and Hyper.

## Prerequisites

Use the package manager [pip] to install the libraries below.

```bash
pip install -r requirements.txt
```
pyenv - python environments managers

## Usage

1. Using terminal, navigate to the tableau_metadata utility in the repository.
1. Create a venv `pyenv install 3.12.4` and then `pyenv virtualenv 3.12.4 tableau_metadata`
1. While in the `tableau_metadata` path, set the local python environment by executing `pyenv local tableau_metadata`
1. Install all the prerequisite packages
1. Run the script `python main.py` and review the output files. or `python main.py --lookback_days 90` if you need lookback for hyper event data.

it will generate several files:
1. `extracted_tdsx/ (a folder)` : This is the folder where the hyper files is stored, useful for the code
2. `full_hyper_tableau_events_database_data.csv`: THis creates a csv file with all the events logs without preprocessing them
3. `merged_workbooks_metadata_<timestamp>.csv`: The final csv data that tells you the woorkbooks metadata merged with events logs. 
4. `TS_Events.hyper.tdsx`: The hyper database file, used by the code.
5. `workbooks_metadata_<timestamp>.json`: 
6. `workbooks_usage_events_count_<timestamp>.csv`: Thsi csv file has cleaned grouped counts events logs from hyper database.
