import sqlparse
import re
import sys
import difflib
import logging
import json
import os
import glob
import time
import traceback
import importlib
from termcolor import colored
from typing import List, Tuple, Dict, Union, Optional, Callable, Any
from sqlparse.sql import IdentifierList, Identifier, Comparison, Where, Parenthesis, TokenList, Function, Case, Operation
from sqlparse.tokens import Keyword, DML, Whitespace, Newline, Punctuation, Number, Literal
import pyodbc

from sql_translate import utils
from sql_translate.query_utils import fetch, run_query
from sql_translate.engine import regex, error_handling

# case insensitive wrapper enforcing that re methods actually have an impact
Regex = regex.Regex()


def validation_runner(operation: Callable) -> Callable:
    """pyodbc decorator

    Args:
        operation (Callable): Input function calling the pyodbc connection

    Returns:
        Callable: Decorated function
    """

    def wrapper(self, *args, **kwargs) -> Any:
        acceptable_errors = ["Failed to reconnect to server."]
        attempt = 1
        max_attempts = 5
        timeout = 3600
        start = time.perf_counter()
        while time.perf_counter() - start < timeout and attempt < max_attempts:
            try:
                output = operation(self)
            except Exception as err:
                if any([acceptable_error in str(err) for acceptable_error in acceptable_errors]):
                    print(
                        f"A known error was encountered during attempt {attempt} after {time.perf_counter() - start} s "
                        f"(max {max_attempts} attempts, timeout at {timeout} s)"
                    )
                    time.sleep(1)
                else:
                    raise Exception(str(err))
            else:
                break
            attempt += 1
        return output
    return wrapper


class _Validator():
    def __init__(self, test_database: str, storage_location: str):
        self.test_database = test_database
        self.storage_location = storage_location


class HiveToPresto(_Validator):
    def __init__(
        self,
        hconn: pyodbc.Connection,
        pconn: pyodbc.Connection,
        test_database: str,
        storage_location: str
    ) -> None:
        super().__init__(test_database, storage_location)

        self.hconn = hconn
        self.pconn = pconn
        self.column_mapping = {
            "float": "double",
            "numeric": "double",
            "tinyint": "tinyint",
            "smallint": "smallint",
            "int": "integer",
            "integer": "integer",
            "bigint": "bigint",
            "string": "string",
            "varchar": "string",
            "timestamp": "timestamp"
        }
        self.validation_timeout = 30  # s, max time to try to validate that the temporary table actually has data in it.
        valid_hive_table_names = r"[\w\d\{\}]+|\`.+\`"
        self.regex_hive_insert = utils.regex_hive_insert
        valid_presto_table_names = r"[\w\d\{\}]+|\"[\w\d\s\{\}]+\""
        self.regex_presto_insert = r"INSERT\s+INTO\s+(?P<database>{vtn})\.(?P<table>{vtn})".format(vtn=valid_presto_table_names)

        self.TableComparator = TableComparator(self.test_database, hconn, pconn)
        self.HiveTableExplorer = utils.HiveTableExplorer(hconn)
        self.ErrorHandlerHiveToPresto = error_handling.ErrorHandlerHiveToPresto()

    def set_paths(self, path_src_sql: str, path_target_sql: str) -> None:
        """Save the paths to the source (Hive) & target (Presto) SQL

        Args:
            path_src_sql (str): Save the path for the source SQL
            path_target_sql (str): Save the path for the target SQL
        """
        self.path_src_sql = path_src_sql
        with open(self.path_src_sql) as f:
            self.src_sql = f.read()

        self.path_target_sql = path_target_sql
        with open(self.path_target_sql) as f:
            self.tgt_sql = f.read()

    def _get_or_create_temp_udf(self, config_data: Dict, path_udf: str) -> Tuple[str, Dict[str, str], Dict[str, str]]:
        """Get or create a temporary udf file based on the real udf.

        Args:
            config_data (Dict): Content of the config.json file
            path_udf (str): path to the python udf

        Returns:
            Tuple[str, Dict[str, str], Dict[str, str]]: Returns:
            - path to the udf to use (either the same one or a temporary one)
            - Mapping between original udf and udf to actually use (the same or a temp one)
            - Query_parameters to use (same or new ones)
        """
        query_parameters = config_data.get("query_parameters", {}).get(os.path.basename(self.path_src_sql), {})
        if not path_udf:  # No UDF provided? Use the default one
            print(f"[WARNING] No UDF provided for this transformation. Therefore, there is no temporary UDF to create.")
            return "", {}, query_parameters
        if query_parameters:  # Some query parameters are defined but not necessarily for the current query
            # I.1 Extract
            with open(path_udf) as f:
                original_udf = f.read()

            # I.2. Transform
            for pattern, replacement in config_data.get("udf_replacements", {}).items():
                original_udf = Regex.sub(
                    pattern,
                    replacement,
                    original_udf,
                    strict=False
                )
            new_udf = original_udf

            # I.3 Load
            new_udf_name = "temp_" + os.path.basename(path_udf)
            path_udf_to_use = os.path.join(os.path.dirname(path_udf), new_udf_name)
            with open(path_udf_to_use, "w") as f:
                f.write(new_udf)
        else:
            path_udf_to_use = path_udf  # No need to convert/translate anything

        udf_mapping = {os.path.basename(path_udf).rstrip(".py"): os.path.basename(path_udf_to_use).rstrip(".py")}
        return path_udf_to_use, udf_mapping, query_parameters

    def evaluate_udfs(self, path_udf: str) -> None:
        """Evaluate the query parameters.
        Handles static values or udf functions that need to be evaluated by loading the relevant python module
        and executing the function.

        Args:
            path_udf (str): Path of the udf (if any)
        """
        # I. Get all parameters to be formatted for the current src SQL
        # I.1. Read query parameters for self.path_src_sql
        path_config = os.path.join(os.path.dirname(self.path_src_sql), 'config.json')
        with open(path_config) as f:
            config_data = json.load(f)

        # I.2. If needed, copy/paste the udf file and substitute custom cluster mentions. Load UDF
        path_udf_to_use, udf_mapping, query_parameters = self._get_or_create_temp_udf(config_data, path_udf)
        if path_udf_to_use:  # A valid UDF was found (in the folder or through the default)
            udf_folder, _ = os.path.split(path_udf_to_use)
            logging.debug(f"[DEBUG] Added {udf_folder} to the path")
            sys.path.insert(0, udf_folder)  # UDFs must be in the source SQL folder

        # II. Set the evaluated the parameters
        if not hasattr(self, "evaluated_query_parameters"):  # Could have been created by self.get_and_create_table_properties
            self.evaluated_query_parameters = {}
        query_parameters = {k: v for k, v in query_parameters.items() if k not in self.evaluated_query_parameters}  # Do not re-evaluate param found

        query_parameters_clean = {}
        # II.1. Subtract the query_parameter, if present, that represent the partition in which to insert.
        # Why? Because we don't want to actualize this value. We want to use the latest partition in the source table.
        for param, value in query_parameters.items():
            if param in self.temp_src_table_properties["latest_partitions"]:
                logging.debug(f"[DEBUG] Found that {param} is a partition value!")
                self.evaluated_query_parameters[param] = self.temp_src_table_properties["latest_partitions"][param]
                logging.debug(f"[DEBUG] /{param}/ has been evaluated to /{self.evaluated_query_parameters[param]}/")
            else:
                logging.debug(f"[DEBUG] Found that {param} is not a partition value.")
                query_parameters_clean[param] = value

        # II.2. The param is not a partition key. It needs to be evaluated from the udf.
        for param, value in query_parameters_clean.items():
            function_call = Regex.search(
                r"^(?P<module>[\w\d]+)\.(?P<function>.+)(?P<args_and_kwargs>\(.*\))$",  # Capture the content of the parenthesis too
                str(value),  # Could be an int from config.json
                strict=False  # Acceptable to have a constant value & not a UDF
            )
            logging.debug(f"[DEBUG] Found function call:{function_call}")
            logging.debug(f"[DEBUG] Will use udf mapping \'{udf_mapping}\' for udf evaluations")
            if function_call:  # Looks like a function call!
                module = importlib.import_module(udf_mapping[function_call["module"]])  # Load module which should be in path
                logging.debug(f"[DEBUG] Successfully imported {module}")
                if function_call["function"] in dir(module):
                    evaluated_value = eval(f"module.{function_call['function']}{function_call['args_and_kwargs']}")
                else:
                    print(f"[WARNING]: Failed to evaluate {udf_mapping[function_call['module']]}\n\t{value} looked like a UDF call but could not be loaded!")
                    evaluated_value = value
            else:
                evaluated_value = value
            logging.debug(f"[DEBUG] Evaluated value for:/{param}/ is /{evaluated_value}/")
            if isinstance(evaluated_value, list) or isinstance(evaluated_value, set) or isinstance(evaluated_value, tuple):
                evaluated_value = sorted(evaluated_value)[-1]  # Select last entry in a list (assumed to be a partition)
                logging.debug(f"[DEBUG] Selecting {evaluated_value}")

            self.evaluated_query_parameters[param] = evaluated_value  # Store parameters as case insensitive

        # III. [Optional] Remove temporary udf that was created
        if path_udf_to_use:  # There was a UDF
            if os.path.basename(path_udf_to_use).startswith("temp_"):
                os.remove(path_udf_to_use)

    def get_and_create_table_properties(self, database: str):
        """Retrieve the properties of the existing table against which the translation will be validated

        Args:
            database (str): Name of the database of interest
        """
        # I. Get table properties from src table
        _, src_table_db, src_table_name, _ = utils.parse_hive_insertion(self.src_sql.replace("${", "{"))
        if "{" in src_table_db:  # Left out as a parameter
            self.evaluated_query_parameters = {Regex.search(r"{(\w+)}", src_table_db).group(1): database}
            src_table_db = Regex.sub(
                r"{\w+}",
                database,
                src_table_db
            )  # If fails, means there was only a single {?

        logging.debug(f"[DEBUG] Found output table in source to be:{src_table_db}.{src_table_name}")
        self.src_table_properties = self.HiveTableExplorer.get_table_properties(f"{src_table_db}.{src_table_name}")

        # II. Create (nearly identical) table properties for temp src & temp tgt tables
        self.temp_src_table_properties = {
            "name": f"validation_hive_{self.src_table_properties['name']}",  # Does NOT have the database name
            "columns": self.src_table_properties["columns"],
            "partition_col_type": self.src_table_properties["partition_col_type"],  # {} if not partitioned else {"name": "data_type"}
            "latest_partitions": self.src_table_properties["latest_partitions"]  # {} if not partitioned else {"name": "latest_partition"}
        }
        self.temp_tgt_table_properties = {
            "name": f"validation_presto_{self.src_table_properties['name']}",  # Does NOT have the database name
            "columns": self.src_table_properties["columns"],
            "partition_col_type": self.src_table_properties["partition_col_type"],  # {} if not partitioned else {"name": "data_type"}
            "latest_partitions": self.src_table_properties["latest_partitions"]  # {} if not partitioned else {"name": "latest_partition"}
        }

    def create_sandbox_tables(self) -> None:
        """Wrapper around _create_sandbox_table to create both Hive & Presto sandbox tables.
        - the Hive table is merely created executing the source Hive SQL
        - the Presto table is created by executing the translation
        """
        # I. Create the temporary Hive table
        self._create_sandbox_table(
            self.temp_src_table_properties["name"],
            self.temp_src_table_properties["columns"],
            self.temp_src_table_properties["partition_col_type"],
            "hive"
        )
        # II. Create the temporary Presto table
        self._create_sandbox_table(
            self.temp_tgt_table_properties["name"],
            self.temp_tgt_table_properties["columns"],
            self.temp_tgt_table_properties["partition_col_type"],
            "presto"
        )

    def upscale_integers(self, data_type: str) -> str:
        """Increase the data type to the maximum allowed in its category.
        Essentially, any integer column becomes a bigint column & any decimal column becomes a double column

        Args:
            data_type (str): Data type for the column considered

        Returns:
            str: New data type
        """
        if data_type in ("int", "tinyint", "smallint", "integer", "bigint"):
            return "bigint"
        if "decimal" in data_type:
            return "double"
        return data_type

    def _create_sandbox_table(self, table_name: str, column_info: Dict[str, str], partition_info: Dict[str, str], engine: str) -> None:
        """Create a sandbox table that will be used for validation

        Args:
            table_name (str): Name of table to create
            column_info (Dict[str, str]): Information about the table
            partition_info (Dict[str, str]): Partition information
            engine (str): Either Hive or Presto
        """
        # I. Drop existing table
        drop_table = f"DROP TABLE IF EXISTS {self.test_database}.{table_name}"
        run_query(drop_table, self.hconn)

        # II. Create new table
        # Define schema & upscale all integers to bigint
        if engine.lower() == "hive":
            column_info = {utils.format_column_name_hive(k): self.upscale_integers(v) for k, v in column_info.items()}
            partition_info = {utils.format_column_name_hive(k): self.upscale_integers(v) for k, v in partition_info.items()}
        elif engine.lower() == "presto":
            column_info = {utils.format_column_name_presto(k): self.upscale_integers(v) for k, v in column_info.items()}
            partition_info = {utils.format_column_name_presto(k): self.upscale_integers(v) for k, v in partition_info.items()}
        formatted_columns = ",\n".join([
            f"{k} {v}"
            for k, v in column_info.items()
        ])

        # Define partition column
        if partition_info:
            partitions = ", ".join([
                f"{k} {v}"
                for k, v in partition_info.items()
            ])
            optional_partitionning = f"PARTITIONED BY ({partitions})"
        else:
            optional_partitionning = ""
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {self.test_database}.{table_name} (\n"
            f"{formatted_columns}\n"
            ")\n"
            f"COMMENT 'Validation table for {table_name}'\n"
            f"{optional_partitionning}\n"
            "STORED AS PARQUET\n"
            f"LOCATION '{self.storage_location}/{table_name}';"
        )

        # Create table
        run_query(ddl, self.hconn)

    @validation_runner
    def insert_into_hive_table(self) -> float:
        """Insert into the Hive validation table in sandbox

        Returns:
            float: Time it took to execute the query (in s)
        """
        # I. Format query parameters
        sql = self.src_sql
        sql = sqlparse.format(sql, strip_comments=True)  # Some comments can be problematic
        sql = sql.replace("${", "{")  # Remove Hue's $ signs if needed
        sql = utils.protect_regex_curly_brackets(sql)  # Protect curly brackets for the upcoming formatting
        sql = Regex.sub(
            r"{\w+}",
            lambda match: match.group().lower(),
            sql,
            strict=False
        )  # Make parameters case insensitive to replace
        sql = sql.format(**self.evaluated_query_parameters)

        # II. Format insert statement
        if self.temp_src_table_properties["latest_partitions"]:
            # Partitions content varies depending on whether we use static or dynamic partitioning
            dynamic_partitioning = False if Regex.search(r"\s+".join(self.regex_hive_insert), sql).groupdict().get("partition_value") else True
            if dynamic_partitioning:
                partitions = " AND ".join(self.temp_src_table_properties["latest_partitions"].keys())
            else:
                partitions = utils.partition_builder(self.temp_src_table_properties, join_key=", ")

            sql = Regex.sub(
                r"\s+".join(self.regex_hive_insert),
                f"INSERT OVERWRITE TABLE {self.test_database}.{self.temp_src_table_properties['name']} PARTITION ({partitions})",
                sql
            )
        else:
            dynamic_partitioning = False  # There are no partitions anyway
            sql = Regex.sub(
                self.regex_hive_insert[0],
                f"INSERT OVERWRITE TABLE {self.test_database}.{self.temp_src_table_properties['name']}",
                sql
            )

        # print(f"Insert into Hive table with:\n{sql}")

        # III. Execute query
        if dynamic_partitioning:
            run_query("SET hive.exec.dynamic.partition.mode=nonstrict", self.hconn)
        else:  # Should be the default
            run_query("SET hive.exec.dynamic.partition.mode=strict", self.hconn)
        start = time.perf_counter()
        run_query(sql, self.hconn)
        duration = time.perf_counter() - start
        print(f"Data was inserted in temp table in {duration:.3f} s with Hive")
        return duration

    @validation_runner
    def insert_into_presto_table(self) -> Tuple[str, float]:
        """Insert into the Presto validation table

        Returns:
            Tuple[str, float]: Validated SQL (likely to be very different from the source) & time it took to execute the query (in s)
        """
        # I. Format query parameters
        sql, original_sql = self.tgt_sql, self.tgt_sql  # Working copy + selectively edited one
        print(f"PARAMS:{self.evaluated_query_parameters}")
        sql = sql.format(**self.evaluated_query_parameters)  # Case insensitive parameters

        # II. Format insert statement in the working copy
        sql = Regex.sub(
            self.regex_presto_insert,
            f"INSERT INTO {self.test_database}.{self.temp_tgt_table_properties['name']}",
            sql
        )

        # III. Execute query & validate that table is not empty
        start = time.perf_counter()
        validated_sql = self._presto_runner(sql, original_sql)  # Returns the validated SQL - likely to be modified
        duration = time.perf_counter() - start
        print(f"Data was inserted in temp table in {duration:.3f} s with Presto")

        validate_table_count = fetch(f"SELECT count(*) FROM {self.test_database}.{self.temp_tgt_table_properties['name']}", self.pconn)
        if validate_table_count[0][0] == 0:
            print(colored(f"WARNING: After inserting into {self.test_database}.{self.temp_tgt_table_properties['name']} the count is still 0", "yellow"))
        return validated_sql, duration

    def _presto_runner(self, sql: str, original_sql: str) -> str:
        """Validate Presto SQL & fix itself when known errors are encountered

        Args:
            sql (str): Working copy of the Presto SQL
            original_sql (str): Original Presto SQL. Edited at the same time as sql.
            Still contains the original table names. Also, parameters are not formatted.

        Raises:
            Exception: A fix was attempted but triggered again the exact same error. The runner exits
            Exception: That Presto error is unknown and cannot be recovered from

        Returns:
            str: Validated version of the original SQL. Unlikely to run as is given that 
            some of the input parameters might be like {param}.
        """
        # First, refactor the final select statement & expand stars/partial stars
        # Re-run until it passes or it fails & cannot be handled
        previous_error_message = ""  # Initialization
        run_count = 1
        while True:
            print(f"Attempt #{run_count} to insert into the Presto table")
            try:
                logging.debug(f"[DEBUG] Presto running:\n{sql}")
                start = time.perf_counter()
                run_query(sql, self.pconn)
                print(f"Presto took {time.perf_counter() - start:.3f} s")
            except Exception as err:  # Cleanup the error & see if it can be recovered
                print(f"Attempt #{run_count} failed!")
                logging.debug(f"[DEBUG] Error is:\n{err}")
                if "Error with HTTP request, response code: 500" in str(err):
                    error_message = "Error with HTTP request, response code: 500"
                else:
                    error_message = str(err).split('")----')[0].split("Presto Query Error: ")[1]
                if error_message == previous_error_message:
                    raise RuntimeError(f"A fix was attempted but it did not solve the issue:\n{error_message}\nAborting.")
                previous_error_message = error_message

                # Fix the issue if it is known
                if error_message == "Error with HTTP request, response code: 500":
                    time.sleep(60)  # Should be enough. Leave time before attempting another run of the SQL
                    continue  # Re-run (once)
                else:
                    sql, original_sql = self.ErrorHandlerHiveToPresto.handle_errors(sql, original_sql, error_message, temp_tgt_table_properties=self.temp_tgt_table_properties)

            else:
                print(f"Attempt #{run_count} passed!")
                if original_sql:
                    return original_sql  # Return the SQL that finally passed
                else:
                    return (
                        "WARNING: THIS IS THE VALIDATED SQL. MODIFICATIONS OF THE ORIGINAL ONE FAILED. "
                        "PLEASE REPLACE RESTORE ANY PARAMETER THAT IS TO BE FORMATTED, EG '2020-10-25' --> {load_date}\n\n"
                    ) + sql
            run_count += 1

    def compare_tables(self) -> float:
        """Wrapper around TableComparator.compare_tables to retrieve the IOU (Intersection Over Union) score used to compare two tables.

        Returns:
            float: IOU score
        """
        iou = self.TableComparator.compare_tables(self.temp_src_table_properties, self.temp_tgt_table_properties)
        if not int(iou) == 1:
            print(f"WARNING: {self.temp_src_table_properties['name']} and {self.temp_tgt_table_properties['name']} are not identical!")
        else:
            print(f"{self.temp_src_table_properties['name']} and {self.temp_tgt_table_properties['name']} are identical!")
        return iou

    def validate_dml(self, path_original: str, path_translation: str, path_udf: str, path_config: str, database: str) -> Tuple[float, float, float]:
        """Main entry point to validate a translation

        Args:
            path_original (str): path of the source (Hive) SQL
            path_translation (str): path of the translated (Presto) SQL
            path_udf (str): Path of the udf
            path_config (str): Path of the config.json file
            database (str): Database in which the validation tables will be created

        Returns:
            Tuple[float, float, float]: Returns:
            - IOU score (1 is perfect match, 0 is no common rows at all between source & validation table)
            - Time it took to run the Hive query
            - Time it took to run the Presto query
        """
        # I. Explore source table
        self.set_paths(path_original, path_translation)
        print(f"Paths set to:\n\t{path_original}\n\t{path_translation}")

        self.get_and_create_table_properties(database)
        print(f"Table properties have been retrieved")

        print(f"Path UDF to evaluate:{path_udf}")
        self.evaluate_udfs(path_udf)
        print(f"UDF {path_udf} have been evaluated")

        # II. Create sandbox tables
        self.create_sandbox_tables()
        print(f"Created sandbox tables {self.temp_src_table_properties['name']} & {self.temp_tgt_table_properties['name']}")

        # III. Fill in the tables
        # Run Presto statement and fill in the table
        validated_sql, presto_run_time = self.insert_into_presto_table()
        print(f"Successfully inserted into {self.temp_tgt_table_properties['name']} with Presto")
        with open(path_translation, "w") as f:  # Export the validated SQL (could be identical or different)
            f.write(validated_sql)

        hive_run_time = self.insert_into_hive_table()
        print(f"Successfully inserted into {self.temp_src_table_properties['name']} with Hive")

        # IV. Compare Presto to Hive table based on metrics
        return self.compare_tables(), hive_run_time, presto_run_time


class TableComparator():  # Should be mostly Hive/Presto agnostic
    def __init__(self, test_database: str, src_conn: pyodbc.Connection, tgt_conn: pyodbc.Connection) -> None:
        self.hconn = src_conn
        self.pconn = tgt_conn
        self.test_database = test_database

    def _sanity_checks(self, table_info_1: Dict, table_info_2: Dict) -> None:
        """Quick helper function checking that both tables have the:
        - same columns
        - same latest partitions

        Args:
            table_info_1 (Dict): Table 1 info
            table_info_2 (Dict): Table 2 info
        """
        assert table_info_1["columns"] == table_info_2["columns"]
        assert table_info_1["latest_partitions"].keys() == table_info_2["latest_partitions"].keys()

    def _get_column_counts(self, table_info_1: Dict, table_info_2: Dict) -> Tuple[List[Tuple[int]], List[Tuple[int]]]:
        """DEPRECATED: Get the column counts from two tables of interest.
        This function is deprecated in favor of the IOU on all rows between both tables.

        Args:
            table_info_1 (Dict): Table 1 info
            table_info_2 (Dict): Table 2 info

        Returns:
            Tuple[List[Tuple[int]], List[Tuple[int]]]: Counts of all columns for each table
        """
        counts = ", ".join([
            f"max(typeof({column})), count({column}), count(distinct {column})"
            for column in table_info_1["columns"]
        ])
        sql = f"SELECT {counts} FROM {{table}}"

        if table_info_1["latest_partitions"]:
            partition_filter = utils.partition_builder(table_info_1, date_cast=True)
            sql += f" WHERE {partition_filter}"
        sql += " LIMIT 1"
        print(sql)

        counts_table_1 = fetch(sql.format(table=f"{self.test_database}.{table_info_1['name']}"), self.pconn)[0]
        counts_table_2 = fetch(sql.format(table=f"{self.test_database}.{table_info_2['name']}"), self.pconn)[0]
        return counts_table_1, counts_table_2

    def _compare_columns_between_two_tables(self, table_info_1: Dict, table_info_2: Dict) -> Dict:
        """DEPRECATED: Compare column counts between two tables
        This function is deprecated in favor of the IOU on all rows between both tables.

        Args:
            table_info_1 (Dict): Table 1 info
            table_info_2 (Dict): Table 2 info

        Returns:
            Dict: Results
        """
        # logging.debug(f"[DEBUG] Comparing {table_info_1['name']} with {table_info_2['name']}")
        counts_table_1, counts_table_2 = self._get_column_counts(table_info_1, table_info_2)
        col_type = {
            1: "count",
            2: "count_distinct"
        }
        return {
            f"{col_type[idx%3]}_{list(table_info_1['columns'])[idx//3]}": {  # Grab column name
                "table_1": counts_table_1[idx],
                "table_2": counts_table_2[idx]
            }
            for idx in range(len(counts_table_1))
            if idx % 3 != 0 and counts_table_1[idx] != counts_table_2[idx]  # Discard data type comparison
        }  # All columns that have different counts accross both tables

    def _compare_rows_between_two_tables(self, table_info_1: Dict, table_info_2: Dict) -> Dict[str, str]:
        """Execute the SQL comparing two tables based on their IOU (Intersection Over Union) score

        Args:
            table_info_1 (Dict): Table 1 info
            table_info_2 (Dict): Table 2 info

        Returns:
            Dict[str, str]: Result of the different components necessary to calculate the IOU score
        """
        from_table_1 = f"{self.test_database}.{table_info_1['name']}"
        from_table_2 = f"{self.test_database}.{table_info_2['name']}"
        if table_info_1["latest_partitions"]:
            partition_filter = utils.partition_builder(table_info_1, date_cast=True)
            from_table_1 += f" WHERE {partition_filter}"
            from_table_2 += f" WHERE {partition_filter}"

        sql = (
            "with\n"
            f"select_distinct_table_1 AS (SELECT distinct * FROM {from_table_1}),\n"
            f"select_distinct_table_2 AS (SELECT distinct * FROM {from_table_2}),\n"
            "table_1_minus_table_2 AS (\n"
            f"\tSELECT * FROM {from_table_1}\n"
            "\texcept\n"
            f"\tSELECT * FROM {from_table_2}\n"
            "),\n"
            "table_2_minus_table_1 AS (\n"
            f"\tSELECT * FROM {from_table_1}\n"
            "\texcept\n"
            f"\tSELECT * FROM {from_table_2}\n"
            "),\n"
            "intersection AS (\n"
            f"\tSELECT * FROM {from_table_1}\n"
            "\tintersect\n"
            f"\tSELECT * FROM {from_table_2}\n"
            ")\n"
            "SELECT '1_count_table_1' AS counts, count(*)\n"
            f"FROM {from_table_1}\n"
            "union\n"
            "SELECT '2_count_table_2' AS counts, count(*)\n"
            f"FROM {from_table_2}\n"
            "union\n"
            "SELECT '3_count_distinct_table_1' AS counts, count(*)\n"
            "FROM select_distinct_table_1\n"
            "union\n"
            "SELECT '4_count_distinct_table_2' AS counts, count(*)\n"
            "FROM select_distinct_table_2\n"
            "union\n"
            "SELECT '5_count_distinct_table_1_minus_table_2' AS counts, count(*)\n"
            "FROM table_1_minus_table_2\n"
            "union\n"
            "SELECT '6_count_distinct_table_2_minus_table_1' AS counts, count(*)\n"
            "FROM table_2_minus_table_1\n"
            "union\n"
            "SELECT '7_count_distinct_intersection' AS counts, count(*)\n"
            "FROM intersection\n"
            "order by counts\n"
        )

        print(sql)
        return dict(fetch(sql, self.pconn))

    def compare_tables(self, table_info_1: Dict, table_info_2: Dict) -> float:
        """Main entry point to compare two tables
        This is used to make sure that the validated SQL produces the same result as the original SQL

        Args:
            table_info_1 (Dict): Table 1 info
            table_info_2 (Dict): Table 2 info

        Returns:
            float: IOU score between the two tables
        """
        # I. Sanity checks
        self._sanity_checks(table_info_1, table_info_2)

        # II. Compare tables
        column_count_differences = self._compare_columns_between_two_tables(table_info_1, table_info_2)
        row_differences = self._compare_rows_between_two_tables(table_info_1, table_info_2)
        print(f"Found column_count_differences:{column_count_differences}")
        print(f"Found row_differences:{row_differences}")

        # III. Display the results
        if column_count_differences:
            print(colored(f"WARNING: Column count is different between {table_info_1['name']} and {table_info_2['name']}", "yellow", attrs=["bold"]))
            print(column_count_differences)
        else:
            print(colored(f"Column count is identical between {table_info_1['name']} and {table_info_2['name']}!"))

        if row_differences["7_count_distinct_intersection"] + \
                row_differences["5_count_distinct_table_1_minus_table_2"] + \
                row_differences["6_count_distinct_table_2_minus_table_1"] == 0 \
                and row_differences["7_count_distinct_intersection"] == 0:
            print(colored(f"WARNING: There are no rows in both tables {table_info_1['name']} and {table_info_2['name']}! Validated with iou = 1"))
            return 1

        iou = row_differences["7_count_distinct_intersection"]/(
            row_differences["7_count_distinct_intersection"] +
            row_differences["5_count_distinct_table_1_minus_table_2"] +
            row_differences["6_count_distinct_table_2_minus_table_1"]
        )
        print(colored(f"IOU (Intersection Over Union): {100*iou:.2f}%", "red"))
        if iou != 1:
            print(colored(f"WARNING: Rows are different between {table_info_1['name']} and {table_info_2['name']}", "yellow", attrs=["bold"]))
            print(row_differences)
        else:
            print(colored(f"Rows are identical between {table_info_1['name']} and {table_info_2['name']}!"))

        return iou
