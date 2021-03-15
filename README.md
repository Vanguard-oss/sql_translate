# sql_translate

sql_translate is a Python library to translate & validate Hive SQL to Presto SQL. 

The translation is done locally, the validation requires a Hive & Presto pyodbc connection.


## Installation

In the near future, a python package version of it will be hosted on PyPi. For the time being, please git clone the repo to use it.

## Usage

### Translation

Translating a query is as simple as:

```python
from sql_translate import translation
HiveToPresto = translation.HiveToPresto()
sql = """
INSERT OVERWRITE TABLE db.table
SELECT
    col1 AS main_col,
    array_contains(col2, 'a') secondary_col
FROM
    my_database.my_table
WHERE
    col1 > 1
    AND col2 IS NOT NULL
ORDER BY
    main_col
LIMIT 10
"""
print(HiveToPresto.translate_statement(sql, has_insert_statement=True))
```

Currently, you **must** indicate whether there is an insert statement in your SQL or not. By default, `has_insert_statement` is `True`. If there is an insert statement and the flag is `False`, any validation will fail. If there isn't an insert statement and the flag is `True`, the translation will fail.


### Validation

Validating a translation requires to have:
- A source Hive SQL file/query
- The corresponding Presto SQL translation
- The ability to create an Hive & Presto pyodbc connection to a cluster

If that is the case, here is how the validation can be done:

```python
from sql_translate import validation
hconn, pconn = ..., ...  # Hive & Presto pyodbc connection respectively
test_database = "my_test_database"  # Name of the database in which validation tables will be created
storage_location = "s3://my_bucket/my_prefix"  # Storage path for the validation tables
HiveToPresto = validation.HiveToPresto(hconn, pconn, test_database, storage_location)

path_original = "my_hive_file.sql"
path_translation = "my_presto_file.sql"
path_udf = "my_udf.py"  # Or just "" if no udf
path_config = "my_config.json"  # Config file needed to run the job
iou, hive_run_time, presto_run_time = HiveToPresto.validate_dml(path_original, path_translation, path_udf, path_config, test_database)
```
The `iou` score is the Intersection Over Union score (also called Jaccard Index) when comparing the rows obtained from the original Hive query and the rows from the Presto query. More information on [IOU](https://en.wikipedia.org/wiki/Jaccard_index). The run times are in seconds.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change. Make sure to checkout the [CONTRIBUTING.md](CONTRIBUTING.md) too.

Please make sure to update tests as appropriate. Typically, this has been used:
```bash
pytest --cov-report term --cov-report html:htmlcov --cov-report xml --cov-fail-under=95 --cov=.
```

## License
[Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0/)

