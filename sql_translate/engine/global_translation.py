import re
from typing import List, Tuple, Optional, Set
import sqlparse
import time
import os
import json
import logging
from tqdm.notebook import tqdm as tq
from sqlparse.tokens import Keyword, DML, Token, Wildcard, Literal, Punctuation, Whitespace, Newline, Comment, Operator
from sqlparse.sql import Identifier, IdentifierList, Function, Parenthesis
from sql_translate.engine import regex
from sql_translate import utils
from termcolor import colored

# case insensitive wrapper enforcing that re methods actually have an impact
Regex = regex.Regex()


class _GlobalTranslator():
    def __init__(self):
        pass


class GlobalHiveToPresto(_GlobalTranslator):
    def __init__(self):
        self.from_language = "Hive"
        self.to_language = "Presto"
        with open(os.path.join(os.path.dirname(__file__), "..", "reserved_keywords", "reserved_keywords.json")) as f:
            self.reserved_keywords = [rkwarg.upper() for rkwarg in json.load(f)["content"]]
        self.gbt = GroupByTranslator()

    def translate_query(self, query: str) -> str:
        """Main runner for the global translation.
        Executes a bunch of O(n) transformations (typically regex substitutions) on the entire SQL.

        Args:
            query (str): Input SQL

        Returns:
            str: Transformed SQL
        """
        query = self._remove_dollar_sign(query)
        query = self._replace_double_quotes(query)
        query = self._replace_back_ticks(query)
        query = self._add_double_quotes(query)
        query = utils.protect_regex_curly_brackets(query)
        query = self._increment_array_indexes(query)
        query = self._cast_divisions_to_double(query)
        query = self._fix_rlike_calls(query)
        query = self._over_shortcut(query)
        query = self._fix_lateral_view_explode_calls(query)
        query = self._fix_double_equals(query)
        query = self._fix_aliasing_on_broadcasting(query)
        query = self._fix_interval_formatting(query)  # WARNING: Must happen !!AFTER!! _fix_aliasing_on_broadcasting
        query = self.gbt.fix_group_by_calls(query)
        return query

    def _remove_dollar_sign(self, query: str) -> str:
        """Remove the dollar sign coming from Hue (on EMR clusters) used to indicate a variable.

        Args:
            query (str): Input SQL

        Returns:
            str: Transformed SQL
        """
        return query.replace("${", "{")

    def _replace_double_quotes(self, query: str) -> str:
        """All double quotes in Hive are replaced by single quotes.
        Double quotes have a different purpose in Presto.
        For more information: https://prestodb.io/docs/current/migration/from-hive.html

        Args:
            query (str): Input SQL

        Returns:
            str: Transformed SQL
        """
        return query.replace('"', "'")  # Double & single quotes are identical in Hive but different in Presto.

    def _replace_back_ticks(self, query: str) -> str:
        """Back ticks from Hive are replaced by double quotes in Presto. There are no back ticks in Presto.
        For more information: https://prestodb.io/docs/current/migration/from-hive.html

        Args:
            query (str): Input SQL

        Returns:
            str: Transformed SQL
        """
        return query.replace('`', '"')  # Handles spaces in column names & any other unicode character.

    def _add_double_quotes(self, query: str) -> str:
        """Identifiers that start with a digit need double quotes in Presto
        For more information: https://prestodb.io/docs/current/migration/from-hive.html

        Args:
            query (str): Input SQL

        Returns:
            str: Transformed SQL
        """
        return Regex.sub(
            r"\b(?P<content>(\d\w*[a-z]\w*))\b",  # WARNING: Does not support column names starting with just a single digit.
            lambda match: f'"{match.groupdict()["content"]}"',  # Surround with double quotes
            query,
            strict=False
        )

    def _increment_array_indexes(self, query: str) -> str:
        """Arrays indexing is 1 based on Presto while it's 0 based in Hive.
        For more information: https://prestodb.io/docs/current/migration/from-hive.html

        Args:
            query (str): Input SQL

        Returns:
            str: Transformed SQL
        """
        return Regex.sub(
            r"\[(\d+)\]",
            lambda match: f"[{int(match.group(1))+1}]",
            query,
            strict=False  # Some files might not have array calls
        )

    def _fix_rlike_calls(self, query: str) -> str:
        """Translate RLIKE to LIKE

        Args:
            query (str): Input SQL

        Returns:
            str: Transformed SQL
        """
        return Regex.sub(
            r"\brlike\b",
            "like",
            query,
            strict=False
        )

    def _over_shortcut(self, query: str) -> str:
        """Frame OVER statement as a function by gluing it to the upcoming parenthesis right after it.

        Args:
            query (str): Input SQL

        Returns:
            str: Transformed SQL
        """
        return Regex.sub(
            r"\bover\s+\(",
            "over(",
            query,
            strict=False
        )

    def _fix_lateral_view_explode_calls(self, query: str) -> str:
        """Lateral view explode in Hive is translated by CROSS JOIN UNNEST in Presto. 
        For more information: https://prestodb.io/docs/current/migration/from-hive.html

        Args:
            query (str): Input SQL

        Returns:
            str: Transformed SQL
        """
        return Regex.sub(
            r"lateral\s+view\s+explode\s*\((?P<explode_content>.+)\)\s+(?P<name>{vtn})(\s+as)\s+(?P<alias>{vtn})".format(vtn=utils.valid_presto_table_names),
            lambda match: f"CROSS JOIN unnest({match['explode_content']}) AS {match['name']} {utils.function_placeholder}({match['alias']})",
            query,
            strict=False
        )

    def _fix_double_equals(self, query: str) -> str:
        """Hive tolerates single or double equales in comparison but Presto is single equal only.

        Args:
            query (str): Input SQL

        Returns:
            str: Transformed SQL
        """
        return Regex.sub(
            r"==",
            "=",
            query,
            strict=False
        )

    def _cast_divisions_to_double(self, query: str) -> str:
        """By default, Presto does an integer division when encountering two integers around a / sign.
        For instance, 3/2 = 1. Therefore, to properly translate it at least one side needs to be cast to double (both sides done here)

        Args:
            query (str): Input SQL

        Returns:
            str: Transformed SQL
        """
        ColumnCaster = utils.ColumnCaster()
        logging.debug("Flattening SQL...")
        start = time.perf_counter()
        flattened_tokens = list(sqlparse.parse(query)[0].flatten())  # Very intensive!
        logging.debug(f"SQL was flattened in {time.perf_counter() - start} s!")
        division_operators = sum([
            True
            for token in flattened_tokens
            if token.ttype == Operator and token.value == "/"
        ])  # Count how many operators there are
        logging.debug(f"Found {division_operators} division operator(s)")

        # Multi stage query copy/paste
        for division_operator in range(division_operators):
            logging.debug(f"Fixing division operation {division_operator}/{division_operators}")
            counter = 0
            idx = 0
            for token in sqlparse.parse(query)[0].flatten():
                if token.ttype == Operator and token.value == "/":
                    if counter == division_operator:
                        query = ColumnCaster.cast_non_trivial_tokens(query, token, idx, "double", {"b_type_0": "", "f_type_0": ""})  # Cast both sides
                        break
                    else:
                        counter += 1
                idx += len(token.value)
        return query

    def _fix_aliasing_on_broadcasting(self, sql: str) -> str:
        """When aliasing a broadcast column, the "AS" needs to be present otherwise sqlparse fails.

        Args:
            query (str): Input SQL

        Returns:
            str: Transformed SQL
        """
        def helper(match: re.match):
            alias = match.groupdict()["alias"]
            if alias.upper() in self.reserved_keywords or alias.upper() in ("ASC", "DESC"):  # List of Presto reserved keywords
                return match.group()  # Not an alias but the continuation of the SQL logic
            else:
                return match.group()[:-len(alias)] + "as " + alias

        numbers = r"\b(\d+\.)?\d+\b"  # Floats & integers
        strings = r"""(`|'|").*?(`|'|")"""  # Careful, non greedy match around quote marks
        return Regex.sub(
            r"""({numbers}|{strings})\s+(?P<alias>`?([a-zA-Z]\w*|"\d\w*")`?)""".format(numbers=numbers, strings=strings),
            lambda match: helper(match),
            sql,
            strict=False
        )

    def _fix_interval_formatting(self, query: str) -> str:
        """Removes the "AS" statement (if any) between an "interval" statement and its alias.

        Args:
            query (str): Input SQL

        Returns:
            str: Transformed SQL
        """
        return Regex.sub(
            r"(interval\s+'.+?'\s+)as\s+(\w+)",
            lambda match: match.group(1) + match.group(2),
            query,
            strict=False
        )

    def move_insert_statement(self, sql: str) -> str:
        """Move the Hive insert statement (which is before the final select) all the way to the top of the file.
        In Presto, the insert statement is the first thing in the query (before any with clause).

        Args:
            query (str): Input SQL

        Returns:
            str: Transformed SQL
        """
        # I. Find partition statement
        partitioned_table = False
        pattern = r"(?P<insert_statement>{base_pattern})".format(base_pattern=r"\s+".join(utils.regex_hive_insert))
        try:
            result = Regex.search(pattern, sql)
            if result["operation"].lower() == "overwrite table":
                print(
                    f"WARNING: {self.to_language} does not support 'INSERT OVERWRITE'. It only supports 'INSERT INTO'.\n"
                    "Replacing 'INSERT OVERWRITE' by INSERT INTO in the translation. Be careful about duplicates when running the query."
                )
            partitioned_table = True
        except Exception:
            pattern = r"(?P<insert_statement>{base_pattern})".format(base_pattern=utils.regex_hive_insert[0])
            result = Regex.search(pattern, sql)

        # III. [Optional] If the table is partitioned, the partition key must be the last column in the final select statement
        # print(f"Before parsing final select:{sql}")
        is_distinct, span, final_select = utils.parse_final_select(sql)
        final_select = [
            [c for c in column if c]  # Filter out the None when there is no alias
            for column in final_select
        ]
        if partitioned_table:
            for idx, column in enumerate(final_select):
                # expression or alias is an exact match -> make it the last column
                if Regex.search(r"^(\w+\.)?{partition_name}$".format(partition_name=result["partition_name"]), column[0], strict=False) \
                        or (result["partition_name"] in column[1] if len(column) == 2 else False):  # If there is an alias/column[1], check if there is a match
                    final_select_clean = final_select[:idx] + final_select[idx+1:]
                    last_column = ",\n" + " AS ".join(final_select[idx]) + "\n"  # Becomes last column, aliased or not
                    break
            else:  # Did not find the partition name. Add the partition value as last column
                final_select_clean = final_select
                last_column = ",\n" + (result["partition_value"] if result["partition_value"] else result["partition_name"]) + "\n"
        else:  # Simple reformatting
            final_select_clean = final_select
            last_column = "\n"

        sql = sql[:span[0]] \
            + "SELECT" + (" DISTINCT" if is_distinct else "") + "\n" \
            + ",\n".join([" AS ".join(n) for n in final_select_clean]) \
            + last_column \
            + sql[span[1]:]
        # print(f"After parsing final select:{sql}")

        # II. Move statement & cleanup
        sql = f'INSERT INTO {result["database"]}.{result["table"]}\n' + \
            sql.replace(result["insert_statement"], "")
        # print(f"After moving statement:{sql}")

        sql = "\n".join([
            row
            for row in sql.split('\n')
            if row.strip()
        ])  # Remove empty lines
        # print(f"After removing empty lines:{sql}")
        return sql


class GroupByTranslator():
    """This object aims at fixing some translation issues related to group by statements.
    """

    def breakdown_real_name(self, token: Token, options: Set[str]) -> Set[str]:
        """Recursively break down the real name in a token into elementary components. One of them must be a column name
        that will try to be found when looking at the group by calls.

        Args:
            token (Token): Original token
            options (Set[str]): growing set

        Returns:
            Set[str]: set of all possible column name
        """
        if token.ttype not in (Punctuation, Whitespace, Newline, Comment, Keyword, Token.Name.Builtin):
            if not isinstance(token, Parenthesis):  # If parenthesis, expand it but do not save it.
                options.add(token.value)
            try:
                for sub_token in token.tokens:
                    self.breakdown_real_name(sub_token, options)
            except:
                return options
            return options
        else:
            return options

    def identifier_parser(self, token: Token) -> List[Optional[Tuple[Set[str], str]]]:
        """Parse an identifier into expression + alias (if any)

        Args:
            token (Token): Identifier to parse

        Returns:
            List[Optional[Tuple[Set[str], str]]]: Single entry list of a tuple containing the expression & the alias from the identifier
        """
        # Extract the real_name, which is everything but the alias
        if token.is_wildcard():  # A wildcard makes the column list incomplete.
            return []  # Will trigger a fast exit
        else:
            expression, alias = utils.extract_alias(token)
            if alias:  # Relies on the observation that get_alias is always right...
                sub_tokens = {
                    sub_token
                    for sub_token in token.tokens
                    if sub_token.value != alias
                    and sub_token.ttype not in (Punctuation, Whitespace, Newline, Comment, Keyword, Token.Name.Builtin)
                }
                column_name_options = {expression}  # Initialize with top level token before drilling down
                for sub_token in sub_tokens:
                    column_name_options.update(self.breakdown_real_name(sub_token, set()))
                return [(column_name_options, alias)]
            else:
                return [(self.breakdown_real_name(token, set()), None)]

    def function_parser(self, token: Token) -> List[Tuple[Set[str], str]]:
        """Parse a function into expression + alias (if any)

        Args:
            token (Token): Function token to parse

        Returns:
            List[Tuple[Set[str], str]]: Single entry list of a tuple containing the expression & the alias from the function
        """
        return [(self.breakdown_real_name(token, set()), None)]  # Functions have no alias, so extract all content. Otherwise they are Identifiers

    def get_columns_in_select(self, token: Token) -> Optional[List[Tuple[Set[str], Optional[str]]]]:
        """Retrieve all the columns in the select statement of a token's parent.

        Args:
            token (Token): A sqlparse token

        Raises:
            SyntaxError: Could not find columns in select statement
            SyntaxError: Could not find a select statement at all

        Returns:
            Optional[List[Tuple[Set[str], Optional[str]]]]: Optional list of select columns broken into their expression & alias (if applicable)
        """
        seen_select = False
        processed_select = False
        to_be_returned = None
        for parent_token in token.parent.tokens:
            if parent_token.ttype == DML and parent_token.value.lower() == "select":  # Not sure to be the right select if there are union/union all!
                seen_select = True
                to_be_returned = None
                processed_select = False  # Restart processing the select if it had been done (means it was not the right one)
                continue

            # Process the select statement even if not sure to be the relevant one for the group by token
            if seen_select and not processed_select:
                if parent_token.ttype == Wildcard:  # Wildcard found. The column list will not be exact. Aborting.
                    to_be_returned = []
                elif parent_token.ttype == Keyword:
                    raise SyntaxError(
                        f"Could not find columns in parent in {token.parent.tokens}"
                        f"Instead, found a Keyword ({parent_token}) before any column could be extracted from select."
                    )
                elif isinstance(parent_token, IdentifierList):  # More than 1 argument
                    print(f"IdentifierList found. Unzipping select columns ({len(parent_token.tokens)} columns)")
                    select_columns = []
                    for id_list_token in parent_token.tokens:  # Columns can be either Identifiers or Functions
                        if id_list_token.ttype == Wildcard:  # Wildcard found. The column list will not be exact. Aborting.
                            to_be_returned = []
                            break
                        elif isinstance(id_list_token, Identifier):
                            if self.identifier_parser(id_list_token):
                                select_columns += self.identifier_parser(id_list_token)
                            else:  # Wildcard found. The column list will not be exact. Aborting.
                                to_be_returned = []
                                break
                        elif isinstance(id_list_token, Function):
                            select_columns += self.function_parser(id_list_token)
                        elif id_list_token.ttype in (Literal.Number.Integer, Literal.Number.Float, Literal.String.Single):
                            select_columns += [(self.breakdown_real_name(id_list_token, set()), None)]
                    to_be_returned = select_columns
                elif isinstance(parent_token, Identifier):  # There is only a single column.
                    to_be_returned = self.identifier_parser(parent_token)
                elif isinstance(parent_token, Function):  # There is a single column in a form of a function
                    to_be_returned = self.function_parser(parent_token)
                elif parent_token.ttype in (Literal.Number.Integer, Literal.Number.Float, Literal.String.Single):
                    to_be_returned = [(self.breakdown_real_name(parent_token, set()), None)]

                if to_be_returned is not None:  # Could be []
                    processed_select = True

            # Found the original group by!
            if parent_token == token:
                if processed_select:
                    return to_be_returned
                else:
                    raise SyntaxError(f"Found a group by call without a relevant select statement. ")

    def validate_column(self, id_list_token: Token, real_name: str, select_columns: List[Tuple[Set[str], Optional[str]]]) -> str:
        """Validate that a given column in a group by clause exists in the select statement.

        Args:
            id_list_token (Token): Group by column being validated
            real_name (str): Expression part of the group by column being validated.
            select_columns (List[Tuple[Set[str], Optional[str]]]): List of expression & aliases found in the select statement

        Returns:
            str: Validated column (might have been re-aliased for clarity)
        """
        real_name_clean = Regex.sub(  # Create a clean version without CTE references
            r"^(\w+\.)",  # Eg: select cte.column from cte -> remove cte when collecting column name.
            "",
            real_name,  # real_name that was extracted
            strict=False
        )
        select_columns_rn = [c[0] for c in select_columns]  # sets of potential column names, based on extracted real name
        if any(real_name in column_name_options for column_name_options in select_columns_rn) \
                or any(real_name_clean in column_name_options for column_name_options in select_columns_rn):  # 2 options
            print(f"SUCCESS: Validated {id_list_token.value}")
            return id_list_token.value  # No change
        else:
            print(
                f"WARNING: {id_list_token.value} does not have its real name ({real_name})"
                f" in select columns:\n{select_columns}"
            )  # maybe an alias though?
            try:
                idx = [c[1] for c in select_columns].index(real_name)  # Different check here: check if on the group by line we called the alias!
            except ValueError:
                print(f"WARNING: {id_list_token.value} does not have its real name {real_name} in the select aliases either!!!")
                return id_list_token.value  # No change
            else:
                print(f"WARNING: {real_name} was actually an alias in the select columns for {select_columns[idx][0]}.")
                output = sorted(select_columns[idx][0], key=len)[-1]  # Get the longest element: that's the real name!
                if id_list_token.get_alias():
                    output += f" as {id_list_token.get_alias()}"  # Re-alias in group by if needed
                return output

    def least_integer_data_type(self, string_rep_integer: str) -> str:
        """Return the most appropriate integer data type in Presto for a given integer

        Args:
            string_rep_integer (str): string representation of an integer

        Returns:
            str: Best Presto integer data type for the input integer.
        """
        value = int(string_rep_integer)
        if -2**7 <= value <= 2**7-1:
            return "tinyint"
        elif -2**15 <= value <= 2**15-1:
            return "smallint"
        elif -2**31 <= value <= 2**31-1:
            return "integer"
        else:
            return "bigint"

    def get_columns_in_group_by_and_validate(self, token: Token, select_columns: List[Tuple[Set[str], Optional[str]]]) -> Tuple[str, int]:
        """Runner processing each group by called found in the input SQL.

        Args:
            token (Token): group by token
            select_columns (List[Tuple[Set[str], Optional[str]]]): Columns found in select statement tied to this group by

        Raises:
            SyntaxError: Found a wildcard in the group by statement, which is not supported.

        Returns:
            Tuple[str, int]: Translated SQL & the original length of the SQL
        """
        seen_group_by = False
        original_length = 0
        translation = ""
        for token_nb, parent_token in enumerate(token.parent.tokens):
            if parent_token == token:  # Found the child group by
                print(f"Found that token {token_nb} is a group by")
                seen_group_by = True
                continue
            if seen_group_by:  # Start counting the length of the section to replace in the original SQL
                print(f"Checking group by column(s). Looking at token {token_nb}: {[parent_token]}")
                original_length += len(parent_token.value)
                if parent_token.ttype == Wildcard:
                    raise SyntaxError(f"Group by statement is a wildcard! This is not allowed.")
                elif parent_token.ttype == Keyword:  # Could not find anything
                    raise SyntaxError(
                        f"Could not find columns in parent in {token.parent.tokens}"
                        f"Instead, found a Keyword ({parent_token}) before any column could be extracted from group by."
                    )
                elif isinstance(parent_token, IdentifierList):  # Validate
                    print(f"IdentifierList found. Unzipping select columns ({len(parent_token.tokens)} columns)")
                    for id_list_token in parent_token.tokens:  # Expand list of Identifiers in group by into id_list_tokens
                        if id_list_token.ttype == Wildcard:
                            raise SyntaxError(f"Found wildcard in group by statement! This is not allowed.")
                        elif isinstance(id_list_token, Identifier):  # Check identifiers in group by IdentifierList
                            if id_list_token.is_wildcard():
                                raise SyntaxError(f"Group by statement contains a wildcard! This is not allowed.")
                            real_name = sorted(self.identifier_parser(id_list_token)[0][0], key=len)[-1]
                            if real_name.isdigit():  # If integer, then needs to be cast!
                                self.validate_column(id_list_token, real_name, select_columns)  # Will raise if impossible
                                translation += f"cast({real_name} as {self.least_integer_data_type(real_name)})"
                                if id_list_token.get_alias():  # Re-alias
                                    translation += f" as {id_list_token.get_alias()}"
                            else:
                                translation += self.validate_column(id_list_token, real_name, select_columns)  # Will raise if impossible
                        elif isinstance(id_list_token, Function):
                            real_name = sorted(self.function_parser(id_list_token)[0][0], key=len)[-1]
                            translation += self.validate_column(id_list_token, real_name, select_columns)  # Will raise if impossible
                        elif id_list_token.ttype in (Literal.Number.Float, Literal.String.Single):
                            print(f"WARNING: In IdentifierList: Broadcast columns of float/strings like /{id_list_token}/ have no impact in a group by statement & should be removed.")
                            translation += id_list_token.value
                        elif id_list_token.ttype == Literal.Number.Integer:
                            print(
                                f"WARNING: In IdentifierList: Broadcast columns of integers like /{id_list_token}/ have no impact in a group by statement & should be removed."
                                f"Presto understands an integer in a group by clause as column number. Therefore, /{id_list_token}/ will be cast to hide it."
                            )
                            translation += f"cast({id_list_token.value} as {self.least_integer_data_type(id_list_token.value)})"
                        else:
                            translation += id_list_token.value
                    break
                elif isinstance(parent_token, Identifier):  # There is only a single column in group by
                    real_name = self.identifier_parser(parent_token)
                    if real_name == []:
                        raise SyntaxError(f"Found wildcard in group by statement! This is not allowed.")
                    real_name = sorted(real_name[0][0], key=len)[-1]
                    if real_name.isdigit():  # If integer, then needs to be cast!
                        translation += f"cast({real_name} as {self.least_integer_data_type(real_name)})"
                        if parent_token.get_alias():  # Re-alias
                            translation += f" as {parent_token.get_alias()}"
                    else:
                        translation += self.validate_column(parent_token, real_name, select_columns)  # Will raise if impossible
                    break
                elif isinstance(parent_token, Function):  # There is a single column in a form of a function
                    real_name = sorted(self.function_parser(parent_token)[0][0], key=len)[-1]
                    translation += self.validate_column(parent_token, real_name, select_columns)  # Will raise if impossible
                    break
                elif parent_token.ttype in (Literal.Number.Float, Literal.String.Single):
                    print(f"WARNING: In Identifier: Broadcast columns of float/strings like /{parent_token}/ have no impact in a group by statement & should be removed.")
                    translation += parent_token.value
                    break
                elif parent_token.ttype == Literal.Number.Integer:
                    print(
                        f"WARNING: In Identifier: Broadcast columns of integers like /{parent_token}/ have no impact in a group by statement & should be removed."
                        f"Presto understands an integer in a group by clause as column number. Therefore, /{parent_token}/ will be cast to hide it."
                    )
                    translation += f"cast({parent_token.value} as {self.least_integer_data_type(parent_token.value)})"
                    break
                else:
                    translation += parent_token.value
        return translation, original_length

    def fix_group_by_calls(self, sql: str) -> str:
        """Top level function for this object.

        Args:
            sql (str): Input SQL to review

        Raises:
            Exception: Translation error (should not happen)

        Returns:
            str: Output SQL
        """
        new_sql = ""
        seen_group_by = False
        length = 0
        for token in sqlparse.parse(sql)[0].flatten():
            if token.ttype == Keyword and token.value.lower() == "group by":
                print("Found a group by! Let's validate its columns against the select part.")
                new_sql += token.value

                select_columns = self.get_columns_in_select(token)

                if select_columns:  # If [], means there was a wildcard in select statement. Then, skip
                    translation, original_length = self.get_columns_in_group_by_and_validate(token, select_columns)
                    new_sql += translation
                    seen_group_by = True  # Set up to skip what's next
                    print(colored(f"Found a group by & its select columns:{select_columns}\nReplacement length: {original_length}, current length: {length}", "red"))
            else:
                if seen_group_by:
                    length += len(token.value)
                    if length == original_length:
                        seen_group_by = False  # Done!
                        length = 0
                    elif length > original_length:
                        raise Exception(
                            f"Could not find the end of the block to replace! Length is now: {length},"
                            f"which is higher than the original_length: {original_length}"
                        )
                else:
                    new_sql += token.value
        return new_sql
