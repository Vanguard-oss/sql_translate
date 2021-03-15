from datetime import datetime as dt
from typing import List, Union, Dict, Tuple, Optional, Callable
import glob
import os
import time
import logging
import json
import re
import shutil
from tqdm.notebook import tqdm as tq
from termcolor import colored
import sqlparse
from sqlparse.sql import IdentifierList
from sqlparse.tokens import Whitespace, Punctuation, Comment, Newline, Keyword, Token, DML
from sql_translate.query_utils import fetch
from sql_translate.engine import regex
import pyodbc
from collections import deque

# Case insensitive wrapper enforcing that re methods actually have an impact
Regex = regex.Regex()
function_placeholder = "zzz_function_placeholder_do_not_use"
d_t = r"\w+(\s*\(\s*\d+\s*(,\s*\d+\s*)?\))?"  # Complete description of a Presto data type
valid_partition_value = r"[\d\w\{\}]+|\'.+\'"
valid_presto_table_names = r"[\w\d\{\}]+|\".+\""
regex_hive_insert = (
    r"INSERT\s+(?P<operation>OVERWRITE\s+TABLE|INTO(\s+TABLE)?)\s+(?P<database>{vtn})\.(?P<table>{vtn})".format(vtn=valid_presto_table_names),
    r"PARTITION\s*\(\s*(?P<partition_name>{vtn})\s*(=\s*(?P<partition_value>{vpv})\s*)?\)".format(vtn=valid_presto_table_names, vpv=valid_partition_value)
)  # Partition value is optional as the dynamic partitioning mode could be enabled.
# Masking converts a special token to a regular identifier (for the parser) by adding the mask in front of it
mask = "z"*10
with open(os.path.join(os.path.dirname(__file__), "masking", "final_select.json")) as f:
    mask_in_final_select = json.load(f)
with open(os.path.join(os.path.dirname(__file__), "masking", "everywhere.json")) as f:
    mask_everywhere = json.load(f)


def display_results(path_results: str, save_files: bool = True) -> Tuple[List, List]:
    """Parse a translation/validation log into sub-logs & displays the outcome with a few stats.
    Three different outcomes exists in the logs:
    1. Success
    2. Error
    3. Timeout

    Args:
        path_results (str): Path to the main result log (a json file)
        save_files (bool, optional): Save the sub logs (success, error & timeout) in separate json files. Defaults to True.

    Returns:
        Tuple[List, List]: Content of the success, error, timeout sub-logs.
    """
    with open(path_results) as f:
        results = json.load(f)
        successes = [entry for entry in results if entry["type"] == "success"]
    errors = [entry for entry in results if entry["type"] == "error"]
    timeouts = [entry for entry in results if entry["type"] == "timeout"]
    print(
        f"Success: {len(successes)}/{len(results)}\n"
        f"Error: {len(errors)}/{len(results)}\n"
        f"Timeout: {len(timeouts)}/{len(results)}\n"
        f"Success rate: {round(100*len(successes)/len(results), 2)}%"
    )

    if save_files:
        prefix = path_results[:-5]
        with open(f"{prefix}_successes.json", "w") as f:
            json.dump(successes, f)
        with open(f"{prefix}_errors.json", "w") as f:
            json.dump(errors, f)
        with open(f"{prefix}_timeouts.json", "w") as f:
            json.dump(timeouts, f)
    return successes, errors, timeouts


def get_path_active_hive_files(paths_job_folders: List[str], path_default_udf: str = "") -> List[Dict]:
    """Get the list of active Hive files to be translated.

    Args:
        paths_job_folders (List[str]): List of folder to explore to retrieve hive SQL files
        path_default_udf (str, optional): Path to default udf in case none are provided in a given folder & a central one is needed. Defaults to "".

    Raises:
        Exception: There can only be up to a single python file in each folder. Otherwise, no idea which one is the udf.

    Returns:
        List[Dict]: Information about the file paths found. Each entry is like:
        {
            "hive": absolute path to the Hive file,
            "presto": absolute path to the translation (Presto) file to be created (or already existing)
            "config": absolute path to the config.json file in the folder of interest,
            "udf": absolute path to the udf
        }
    """
    ddl_keywords = ("grant", "role", "create", "drop")
    path_info = []
    for path_folder in tq(paths_job_folders):
        # Folder level info
        # Extract path config file
        path_config = os.path.join(path_folder, 'config.json')
        with open(path_config) as f:
            config = json.load(f)
        # Extract path python file that would be considered the UDF
        path_udf = glob.glob(os.path.join(path_folder, '*.py'))
        if len(path_udf) == 0:  # No python file in the folder, so no UDF.
            path_udf = path_default_udf
        elif len(path_udf) == 1:  # There is only one python file, so it is assumed to be the UDF
            path_udf = path_udf[0]
        else:  # There are many python files. Filter out the one known to be useless.
            # Delete all temp_ udf that could exist
            file_names = {os.path.basename(p).lstrip("temp_") for p in path_udf}
            for path in path_udf:
                if os.path.basename(path).startswith("temp_") and os.path.basename(path).lstrip("temp_") in file_names:
                    os.remove(path)  # Delete temp udf
            path_udf = glob.glob(os.path.join(path_folder, '*.py'))
            if len(path_udf) > 1:
                raise Exception(
                    "There can only be one python file in the folder, serving as udf. "
                    f"However, found {len(path_udf)} python files in {path_folder}:\n{path_udf}"
                )
            else:
                path_udf = path_udf[0]

        # Collect all hive files & generate their presto path
        file_names_to_consider = [
            file_name
            for file_name in config["run_order"]
            if file_name.endswith(".hive") and not "ddl" in file_name
        ]
        for file_name in file_names_to_consider:
            absolute_path = os.path.join(path_folder, file_name)
            with open(absolute_path) as f:
                sql = f.read()
            for query in sqlparse.parse(sql):  # Extract all queries from statement
                for token in query.flatten():
                    if token.ttype in (sqlparse.tokens.Keyword, sqlparse.tokens.DDL) and token.value.lower() in ddl_keywords:
                        break
                else:  # No DDL keyword found in this query
                    continue  # Move to the next query in the statement
                break
            else:
                path_info.append({
                    "hive": absolute_path,
                    "presto": Regex.sub(r".hive$", ".presto", absolute_path),
                    "config": path_config,
                    "udf": path_udf
                })

    print(f"Found {len(path_info)} active files!")
    return path_info


def protect_regex_curly_brackets(query: str) -> str:
    """Protects curly brackets coming from regex patterns from interfering with str.format python call.
    Example: select regexpr(a, '^[0-9]{4}-[0-9]{2}-[0-9]{2}') --> select regexpr(a, '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}')

    Args:
        query (str): Input query

    Returns:
        str: Protected query
    """
    return Regex.sub(
        r"({\d+})",  # WARNING: Does not support column names starting with just a single digit.
        lambda match: "{" + match.group() + "}",
        query,
        strict=False
    )


def extract_alias(token: Token) -> Tuple[str, Optional[str]]:
    """Reliably separate the expression & alias from a sqlparse identifier

    Args:
        token (Token): Input sqlparse token

    Returns:
        Tuple[str, Optional[str]]: expression, alias from the identifier
    """
    # I. Extract alias
    double_quotes = '"' if re.search(r"\"", token.value) else ""  # get_alias removes double quotes but not backticks
    try:
        alias = token.get_alias()
    except AttributeError:  # Not an identifier or anything else that could have an alias
        alias = None

    # II. Extract the expression
    if alias:
        if double_quotes:  # Starts with a digit & would require double quotes
            alias = f'{double_quotes}{alias}{double_quotes}'
        expression = Regex.sub(
            r"\s+(as\s+)?{alias}$".format(alias=re.escape(alias)),
            "",
            token.value
        )  # Extract the real name that will be broken down in a set of elementary components
    else:
        expression = token.value
    return expression, alias


def partition_builder(table_params: Dict[str, str], join_key: str = " AND ", date_cast: bool = False) -> str:
    """Creates a SQL string representing a filter on partition keys based on the parameters extracted from the table.
    Example: {"latest_partitions": {"load_date": "2020-10-25"}, "partition_col_type": {"load_date": "date"}} -->
    load_date='2020-10-25'
    If date_cast == True --> load_date=date('2020-10-25')

    Args:
        table_params (Dict[str, str]): Various parameters extracted from the table
        join_key (str, optional): string used to join the various partition keys. Defaults to " AND ".
        date_cast (bool, optional): Cast partition value to date using the date SQL function. Defaults to False.

    Returns:
        str: String that can be used as a filter for partition values.
    """
    string_types = ("string", "char", "timestamp")
    partition_values = []
    for key, value in table_params["latest_partitions"].items():
        if any([string_type in table_params["partition_col_type"][key] for string_type in string_types]):
            partition_values.append(f"{key}='{value}'")  # Add the single quotes around the value
        elif table_params["partition_col_type"][key] == "date":  # date has special treatment in table compare but not in partition call
            if date_cast:
                partition_values.append(f"{key}=date('{value}')")
            else:
                partition_values.append(f"{key}='{value}'")
        else:  # bigint or else
            partition_values.append(f"{key}={value}")
    return join_key.join(partition_values)


def char_to_number(value: str, required_type: str) -> str:
    """Converts the string representation of a number (int or float) to a number

    Args:
        value (str): String representation of a number
        required_type (str): Output type desired (bigint or double)

    Raises:
        Exception: The conversion to a python integer (using int built in method) failed
        Exception: The conversion to a python float (using float built in method) failed
        NotImplementedError: required_type was neither bigint nor double

    Returns:
        str: Numerical representation of the input string
    """
    value = value.strip("'").strip('"')  # Remove potential character markers
    if required_type == "bigint":
        try:
            assert f"{int(value)}" == value  # Make sure the str representation does not change
            return value
        except (TypeError, AssertionError, ValueError):
            msg = (
                f"A 'bigint' type is expected by Presto for this function, but {value} "
                "was provided which either cannot be casted or does not seem to represent an integer."
            )
            raise Exception(msg)
    elif required_type == "double":
        try:
            assert f"{float(value)}" == value
            return value
        except (TypeError, AssertionError, ValueError):
            msg = (
                f"A 'double' type is expected by Presto for this function, but {value} "
                "was provided which either cannot be casted or does not seem to represent a float."
            )
            raise Exception(msg)
    else:
        raise NotImplementedError


def parse_describe_formatted(query_output: List[Tuple[str]]) -> Dict:
    """Converts to json like format the output of Hive's DESCRIBE FORMATTED
    WARNING: Handles section & sub sections but not deeper.

    Args:
        query_output (List[Tuple[str]]): Result of the DESCRIBE FORMATTED query

    Returns:
        Dict: Json like output
    """
    if not query_output:
        raise RuntimeError("[ERROR] The output of 'describe formatted' that was provided is empty. That is not supposed to happen.")
    describe_json = {}
    sub_section = None
    for row in query_output:
        row = [
            field.strip().rstrip(':') if isinstance(field, str) else None
            for field in row
        ]  # Clean up all the fields
        if not any(row):  # empty line
            continue
        if '# ' in row[0]:
            new_header = row[0].lstrip('# ')
            if new_header in describe_json:
                sub_section = new_header
                describe_json[section_name][sub_section] = {}
            else:
                section_name = new_header
                describe_json[section_name] = {}
                sub_section = None
        elif row[0]:
            if sub_section:
                describe_json[section_name][sub_section][row[0]] = row[1]
            else:
                describe_json[section_name][row[0]] = row[1]
    return describe_json


def decode_utf8(text: str) -> str:
    """Eliminates encoded UTF-8 symbols in a text
    Example: 2020%2D03%2D25 16%3A25%3A17%3A%2E0 --> 2020-03-25 16:25:17.0

    Args:
        text (str): Input text

    Returns:
        str: Decoded text
    """
    with open(os.path.join(os.path.dirname(__file__), "decode_utf8", "lookup_table.json")) as f:
        lookup_table = json.load(f)
    return Regex.sub(
        r"(?P<utf8>%[\dA-F]{2})",
        lambda x: f"{lookup_table[x.group(1)]}",
        text,
        strict=False  # Very few input files will have corrupted utf8 characters
    )


def parse_hive_insertion(sql: str) -> Tuple[str, str, str, Dict]:
    """Parse the insert statement in the Hive SQL

    Args:
        sql (str): Input SQL

    Returns:
        Tuple[str, str, str, Dict]: Outputs:
        - operation: overwrite or into table basically
        - database name
        - table name
        - partition info
        Example: "overwrite", "output_db", "output_table", {"partition_name": "load_date", "partition_value": "'2020-03-25'"}

    """
    # I. Set up
    partition_info = {}

    # II. Regex searches
    pattern = r"\s+".join(regex_hive_insert)
    # logging.debug(f"[DEBUG] pattern:{pattern}")
    try:
        result = Regex.search(pattern, sql)  # Raises in strict mode if not partitioned
    except Exception:  # II.1. Not partitioned. Shall not fail again
        result = Regex.search(regex_hive_insert[0], sql)
    else:  # II.2. Partitionned tables
        partition_info["partition_name"] = result["partition_name"].strip()
        partition_info["partition_value"] = result["partition_value"].strip() if result["partition_value"] else None

    # III. Parse result
    operation = result['operation'].split(" ")[0].lower()  # Remove TABLE to only keep overwrite or into
    return operation, result['database'].lower(), result['table'].lower(), partition_info


def parse_final_select(sql: str) -> Tuple[bool, List[int], Dict[str, Optional[str]]]:
    """Parses the final select statement
    Extracts:
    - whether it's a "distinct" call or not
    - the start & end index in the string representation of the SQL
    - the processed tokens (split into expression and optionally alias)

    Args:
        sql (str): Input SQL to parse

    Returns:
        Tuple[bool, List[int], Dict[str, Optional[str]]]: distinct flag, start/end & processed tokens.
    """
    # Gets all info from final select (real name + alias). Flag is_distinct
    # 2. Get all tokens between DML select & keyword from
    # 2.1 Skip until final select
    position = 0
    for token in sqlparse.parse(sql)[0].tokens:
        if token.ttype == DML and token.value.lower() == "select":
            start = position
            break
        position += len(token.value)
    final_select = Regex.sub(
        r"\b({mask_in_final_select})\b".format(mask_in_final_select="|".join(mask_in_final_select)),  # Mask keywords as identifiers
        lambda match: mask + match.group(1),
        sql[start:],
        strict=False
    )

    # 2.2 Final select
    is_distinct = False
    select_tokens = []
    position = 0
    for token in sqlparse.parse(final_select)[0].tokens:  # Restart at the select statement
        if token.ttype == Keyword and token.value.lower() == "from":
            end = position
            break
        elif token.ttype == DML and token.value.lower() == "select":  # Should be the first token
            pass
        elif token.ttype == Keyword and token.value.lower() == "distinct":
            is_distinct = True
        elif token.ttype in (Whitespace, Newline, Punctuation, Comment):
            pass
        elif isinstance(token, IdentifierList):
            for subtoken in token.tokens:
                if subtoken.ttype in (Whitespace, Newline, Punctuation, Comment):
                    pass
                else:
                    select_tokens.append(extract_alias(subtoken))
        else:
            select_tokens.append(extract_alias(token))
        position += len(token.value)

    # 3. Merge entries with or/and & unmask
    def unmask(text: str) -> str:
        new_text = Regex.sub(
            r"\b({unmask_in_final_select})\b".format(unmask_in_final_select="|".join([mask + t for t in mask_in_final_select])),  # unmask
            lambda match: match.group(1).split(mask)[1],
            text,
            strict=False
        )
        return len(text) - len(new_text), new_text

    select_tokens_clean = []
    while select_tokens:
        expression, alias = select_tokens.pop(0)
        length_change, expression = unmask(expression)
        end -= length_change
        if expression.lower() in ("or", "and", "over"):  # OVER needed for embedded window functions...but could be an alias...
            next_expression, next_alias = select_tokens.pop(0)  # Get the next argument
            length_change, next_expression = unmask(next_expression)
            end -= length_change
            select_tokens_clean[-1][0] += f' {expression} {next_expression}'  # Append to last argument
            select_tokens_clean[-1][1] = next_alias  # Get the alias of the next argument
        else:
            select_tokens_clean.append([expression, alias])
    assert sql.lower()[start:].startswith("select")
    assert sql.lower()[start+end:].startswith("from")
    return is_distinct, [start, start+end], select_tokens_clean


def format_column_name_hive(column_name: str) -> str:
    """Surround column name with back ticks if needed to make it Hive compatible.

    Args:
        column_name (str): Column name

    Returns:
        str: Formatted column name with back ticks as needed
    """
    if Regex.search(r"[^\w]", column_name, strict=False):  # Found a non alpha numerical character -> needs backticks
        return f"`{column_name}`"
    else:
        return column_name


def format_column_name_presto(column_name: str) -> str:
    """Surround column name with back ticks/double quotes if needed to make it Presto compatible.

    Args:
        column_name (str): Column name

    Returns:
        str: Formatted column name with back ticks/double quotes as needed.
    """
    if Regex.search(r"[^\w]", column_name, strict=False):  # Found a non alpha numerical character -> needs backticks
        return f"`{column_name}`"
    elif Regex.search(r"^\d", column_name, strict=False):
        return f'"{column_name}"'
    else:
        return column_name


class HiveTableExplorer():
    def __init__(self, hconn: pyodbc.Connection) -> None:
        self.hconn = hconn
        self.partition_black_list = (
            "etl_state=history",
        )

    def _get_latest_partitions(self, table_name: str) -> Dict[str, str]:
        """Get the latest partition for a given table

        Args:
            table_name (str): Name of the table to query

        Raises:
            ValueError: Multiple partition keys are not supported
            ValueError: Table is partitionned but does not contain any data.

        Returns:
            Dict[str, str]: Partition key & value for the latest partition in table
        """
        partitions = fetch(f"SHOW PARTITIONS {table_name}", self.hconn)
        try:
            assert "/" not in partitions[0][0]
        except AssertionError:
            msg = (
                f"Multiple partition keys detected: {partitions[0][0]}. "
                "This behavior is not yet supported."
            )
            raise ValueError(msg)
        except IndexError:
            raise ValueError(f"Table {table_name} is partitioned but does not contain any data!")

        partitions = [
            decode_utf8(p[0])  # Remove encoded utf8 symbols
            for p in partitions
            if p not in self.partition_black_list
        ]
        partition_key, partition_value = sorted(partitions, reverse=True)[0].split("=")
        return {partition_key: partition_value}

    def _describe_formatted(self, table_name: str) -> Dict:
        """Run describe formatted statement on a given table

        Args:
            table_name (str): Name of the table to query

        Returns:
            Dict: Get the formatted & cleaned up output of the describe formatted statement
        """
        describe_formatted = fetch(f"DESCRIBE FORMATTED {table_name}", self.hconn)
        return parse_describe_formatted(describe_formatted)

    def get_table_properties(self, table_name: str) -> Dict:
        """Extracts basic table properties
        Latest partitions are stored as is or surrounded by single quote marks if string-type
        Example: date_loaded=2020-03-25 is extracted as {"date_loaded": "'2020-03-25'"}

        Args:
            table_name (str): Name of the table of interest

        Returns:
            Dict: Table properties
        """
        table_description_clean = self._describe_formatted(table_name)
        table_properties = {
            "name": table_name.split(".")[-1],  # ONLY KEEP THE NAME, NOT DATABASE.
            "table_location": table_description_clean["Detailed Table Information"]["Location"],
            "columns": {k.lstrip('_'): v for k, v in table_description_clean["col_name"].items()}
        }
        if table_description_clean.get("Partition Information"):
            latest_partitions = self._get_latest_partitions(table_name)
            table_properties["partition_col_type"] = table_description_clean["Partition Information"]["col_name"]
            table_properties["latest_partitions"] = {
                key: str(value) if any([k in table_description_clean["Partition Information"]["col_name"][key] for k in ("string", "char", "timestamp")]) else value
                for key, value in latest_partitions.items()
            }  # Add quote marks around value as needed
        else:  # Populate the partition fields with empty dictionaries
            table_properties["partition_col_type"] = {}
            table_properties["latest_partitions"] = {}

        return table_properties


class ColumnCaster():
    def __init__(self) -> None:
        pass

    def get_problematic_token(self, sql: str, line: int, column: int) -> Tuple[sqlparse.sql.Token, int]:
        """Retrieves the token at a given line/column in the SQL
        This function is heavily used to handle errors from Presto.

        Args:
            sql (str): Input SQL
            line (int): Line of the token of interest
            column (int): Column of the token of interest

        Raises:
            AttributeError: Could not expand a token into sub tokens. Should not happen.
            ValueError: Could not find the token of interest. Should not happen

        Returns:
            Tuple[sqlparse.sql.Token, int]: Token & linear index (index in the string) at which it happened
        """
        current_line, current_column, idx = 0, 0, 0
        stack = deque(sqlparse.parse(sql)[0].tokens[::-1])
        while stack:
            new_token = stack.pop()
            extra_lines = new_token.value.count("\n")
            extra_columns = len(new_token.value.split("\n")[-1])
            # logging.debug(f"[DEBUG] Found {extra_columns} extra columns for token /{new_token}/")
            if current_line == line and current_column == column:  # New token is the one of interest!
                return new_token, idx
            # logging.debug(f"[DEBUG] token:{[new_token]}|current_line:{current_line}|current_column:{current_column}")
            if current_line + extra_lines > line \
                    or (current_line + extra_lines == line and (extra_columns if extra_lines else current_column + extra_columns) > column):
                try:
                    stack += new_token.tokens[::-1]  # Expand new_tokens and add it back to the stack
                except AttributeError:
                    # print(f"Location: current_line:{current_line}|extra_lines:{extra_lines}|current_column:{current_column}|extra_columns:{extra_columns}\nTarget:line:{line}|column:{column}")
                    raise AttributeError(f"[DEBUG] Could not expand new_token {[new_token]} (ttype: {new_token.ttype}) into sub tokens. Stack content:\n{stack}")
            else:  # The token of interest was not inside. Actually increment counters
                current_line += extra_lines
                current_column = extra_columns if extra_lines else current_column + extra_columns
                # logging.debug(f"[DEBUG] current_column is now {current_column} after adding token /{new_token}/")
                idx += len(new_token.value)
        else:
            raise ValueError(f"Could not find a token that started at line {line} and column {column}!")

    def _get_data_types(self, groupdict: Dict, direction: str) -> List[str]:
        """Transform the groupdict result

        Args:
            groupdict (Dict): groupdict result from regex match in error_handling
            direction (str): left or right

        Returns:
            List[str]: Transformed list of data types
        """
        data_types = []
        while True:
            try:
                data_types.append(groupdict[f"{direction}_type_{len(data_types)}"])
            except KeyError:
                return data_types if data_types else [""]

    def _light_cast(self, token: Token, cast_to: str, data_type: str) -> str:
        """Cast only when really necessary to improve the quality of the translation

        Args:
            token (Token): Input token to cast
            cast_to (str): Data to cast to
            data_type (str): Input data type

        Returns:
            str: Translated/cast output
        """
        if cast_to in ("varchar", "tinyint", "smallint", "integer", "int", "bigint", "real", "double"):  # No parenthesis to specify stuff
            if data_type.startswith(cast_to):  # Already proper data type, no need to cast again
                return token.value
        elif any(cast_to.startswith(t) for t in ("decimal", "char")):
            if data_type == cast_to:  # Needs exact match for this one
                return token.value
        return f"cast({token} AS {cast_to})"

    def _find_non_trivial_tokens(self, parent_tokens: List[sqlparse.sql.Token], cast_to: str, data_types: List[str], count_non_trivial_tokens: int) -> Tuple[List[str], int]:
        """Find the non trivial tokens in a list of sqlparse tokens

        Args:
            parent_tokens (List[sqlparse.sql.Token]): List of sqlparse tokens
            cast_to (str): Data type to cast to
            data_types (List[str]): Data types found
            count_non_trivial_tokens (int): Number of non trivial tokens to return from the list

        Raises:
            ValueError: Not enough non trivial tokens were found

        Returns:
            Tuple[List[str], int]: Translated non trivial tokens & span the original tokens took in the input SQL
            Example: if you had "where a = 2" replaced by "where cast(a AS varchar) = 2", then span = 1 (just 2 for "a")  
        """
        non_trivial_counter, identifier_counter = 0, 0
        cast_tokens = []
        span = 0
        if non_trivial_counter == count_non_trivial_tokens:  # count_non_trivial_tokens == 0
            return cast_tokens, span
        for sub_token in parent_tokens:  # Explore backward to find the first non trivial token
            span += len(sub_token.value)
            if sub_token.ttype in (Whitespace, Punctuation, Comment, Newline):  # Trivial token
                cast_tokens.append(sub_token.value)
            else:  # Found a non trivial token!
                if sub_token.ttype != Keyword:
                    # logging.debug(f"[DEBUG]data_types:{data_types}|cast_tokens:{cast_tokens}|identifier_counter:{identifier_counter}|non_trivial_counter:{non_trivial_counter}")
                    cast_tokens.append(self._light_cast(sub_token, cast_to, data_types[identifier_counter]))
                    identifier_counter += 1
                else:
                    cast_tokens.append(sub_token.value)  # Counts as non trivial but does not get cast
                non_trivial_counter += 1
                if non_trivial_counter == count_non_trivial_tokens:
                    return cast_tokens, span  # Include current token
        else:
            raise ValueError(f"Did not find all the required {count_non_trivial_tokens} non trivial tokens in {parent_tokens}!")

    def cast_non_trivial_tokens(
        self,
        sql: str,
        token: Token,
        idx: int,  # linear index in SQL
        cast_to: str,  # Type to cast to. Replacement is non strict.
        groupdict: Dict,
        count_backward_tokens: int = 1,  # Grab first non trivial token before the marker
        count_forward_tokens: int = 1  # Grab first non trivial token post marker
    ) -> str:  # Translated SQL
        """Main entry point for the object.
        Mostly workflow automation for the different object methods.

        Args:
            sql (str): Input SQL
            token (Token): Central token around which the non trivial tokens will be looked for
            idx (int): Linear index of where the token is in the SQL
            cast_to (str): Data type to be cast to
            groupdict (Dict): groupdict attribute from the regex match
            count_backward_tokens (int, optional): Number of non trivial tokens to look for before the token. Defaults to 1.
            count_forward_tokens (int, optional): Number of non trivial tokens to look for after the token. Defaults to 1.

        Returns:
            str: Translated SQL
        """
        # Get required count of non trivial tokens before and after
        forward_types, backward_types = self._get_data_types(groupdict, "f"), self._get_data_types(groupdict, "b")
        child_idx = token.parent.tokens.index(token)
        cast_backward_tokens, backward_span = self._find_non_trivial_tokens(token.parent.tokens[child_idx-1::-1], cast_to, backward_types, count_backward_tokens)
        cast_forward_tokens, forward_span = self._find_non_trivial_tokens(token.parent.tokens[child_idx+1:], cast_to, forward_types, count_forward_tokens)

        # Stitch the SQL back together
        return sql[:idx-backward_span] + "".join(cast_backward_tokens[::-1]) + token.value + "".join(cast_forward_tokens) + sql[idx+forward_span+len(token.value):]
