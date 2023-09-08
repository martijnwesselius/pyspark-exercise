# PySpark Exercise

This is a small exercise which I had to make as an assignment for a job application. The program joins two specified CSV files using PySpark and applies some minor transformations and filtering.


<!-- ## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install foobar.

```bash
pip install foobar
``` -->

## Usage

The program takes three inputs from the user:
- **fpath_1**: path to the second CSV file
- **fpath_2**: path to the second CSV file
- **countries**: one or more countries to filter from the data (France, Netherlands, United Kingdom, United States)

The program is for instance started as follows:

```python
join.py --fpath_1 'data/dataset_one.csv' --fpath_2 'data/dataset_two.csv' --countries 'United Kingdom' 'Netherlands'
```