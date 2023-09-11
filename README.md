# PySpark Exercise

This is a small programming exercise which I made as an assignment for a job application. The program reads two CSV files provided by the user. These files are red as two Spark DataFrames and joined together into a single DataFrame. The columns of this DataFrame are renamed and the rows are filtered on the countries provided by the user. The resulting DataFrame is then saved as an CSV file and stored in the **client data** directory.

## Usage

The program takes three inputs from the user:
- **fpath_1**: path to the second CSV file
- **fpath_2**: path to the second CSV file
- **countries**: one or more countries to filter from the data (France, Netherlands, United Kingdom, United States)

The program is for instance started as follows:

```python
join.py --fpath_1 'data/dataset_one.csv' --fpath_2 'data/dataset_two.csv' --countries 'United Kingdom' 'Netherlands'
```