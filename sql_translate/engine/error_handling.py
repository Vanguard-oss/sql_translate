from typing import Tuple, List, Optional, Dict
import sqlparse.sql
from sqlparse.sql import IdentifierList, Identifier, Where, Parenthesis, TokenList, Function, Case, Operation
import sqlparse.tokens
from sqlparse.tokens import Keyword, DML, CTE, Whitespace, Newline, Punctuation, Number, Literal, Comment, Name, Operator, Wildcard, Token
import sqlparse
import re
import logging
from sql_translate.engine import regex
from sql_translate import utils

# case insensitive wrapper enforcing that re methods actually have an impact
Regex = regex.Regex()


class _ErrorHandler():
    def __init__(self) -> None:
        pass


class ErrorHandlerHiveToPresto(_ErrorHandler):
    def __init__(self) -> None:
        # Covers all Presto data types from single word (eg: integer) to composed (eg: decimal(22, 2))
        self.known_issues = {
            r"line (?P<line>\d+):(?P<column>\d+): Cannot cast timestamp to (?P<integer_type>tinyint|smallint|integer|int|bigint) \(\d+\)": self._cast_timestamp_to_epoch,
            r"line (?P<line>\d+):(?P<column>\d+): Cannot cast (?P<source_type>{d_t}) to (?P<target_type>{d_t}) \(\d+\)".format(d_t=utils.d_t): self._cannot_cast_to_type,
            r"line (?P<line>\d+):(?P<column>\d+): '(>|<|[><!]?=)' cannot be applied to (?P<b_type_0>{d_t}), (?P<f_type_0>{d_t}) \(\d+\)".format(d_t=utils.d_t): self._cast_both_sides,
            r"Mismatch at column (?P<column>\d+): \'(?P<name>\w+)\' is of type (?P<ddl_type>{d_t}) but expression is of type (?P<expr_type>{d_t}) \(\d+\)".format(d_t=utils.d_t): self._column_type_mismatch,
            r"line (?P<line>\d+):(?P<column>\d+): Unexpected parameters \((?P<parameters>.*)\) for function (?P<function_name>\w+)": self._unexpected_parameters,
            r"Table '(?P<table>\w+.\w+)' not found \(\d+\)": self._table_not_found,
            r"line (?P<line>\d+):(?P<column>\d+): Cannot check if (?P<b_type_0>{d_t}) is BETWEEN (?P<f_type_0>{d_t}) and (?P<f_type_1>{d_t}) \(\d+\)".format(d_t=utils.d_t): self._between,
            r"line (?P<line>\d+):(?P<column>\d+): IN value and list items must be the same type: (?P<type>{d_t}) \(\d+\)".format(d_t=utils.d_t): self._cast_in,
            r"line (?P<line>\d+):(?P<column>\d+): value and result of subquery must be of the same type for IN expression: (?P<b_type_0>{d_t}) vs (?P<f_type_0>{d_t}) \(\d+\)".format(d_t=utils.d_t): self._cast_in_subquery,
            r"line (?P<line>\d+):(?P<column>\d+): All CASE results must be the same type: (?P<type>{d_t}) \(\d+\)".format(d_t=utils.d_t): self._case_statements,
            r"line (?P<line>\d+):(?P<column>\d+): All COALESCE operands must be the same type: (?P<type>{d_t}) \(\d+\)".format(d_t=utils.d_t): self._coalesce_statements

        }
        self.ColumnCaster = utils.ColumnCaster()

    def _cast_timestamp_to_epoch(self, sql: str, result: re.match, **kwargs) -> str:
        """Replaces cast to integer of a timestamp by a conversion to number of seconds elapsed since the epoch.
        Example: select cast(a as integer) --> line 1:8: Cannot cast timestamp to integer (1)
        Fix: select to_unixtime(a)

        Args:
            sql (str): SQL to fix
            result (re.match): Regex match object

        Returns:
            str: Fixed SQL
        """
        line, column = int(result["line"]) - 1, int(result["column"]) - 1  # Presto to python array notation
        token, idx = self.ColumnCaster.get_problematic_token(sql, line, column)  # Should return an identifier list or identifier
        expression, alias = utils.extract_alias(token)
        cast_content = Regex.search(
            r"cast\s*\((?P<core_expression>.*?)\s+as\s+{type}\s*\)".format(type=result["integer_type"]),
            expression
        )
        expression = f"to_unixtime({cast_content['core_expression']})"
        if alias:
            expression += f" AS {alias}"
        return sql[:idx] + expression + sql[idx + len(token.value):]

    def _coalesce_statements(self, sql: str, result: re.match, **kwargs) -> str:
        """Casts all operands in a coalesce statement to the same data type.
        Coalesce operands need to have the same data type. The data type of the first operand is used.
        Example: select coalesce('1', 1) --> line 1:22: All COALESCE operands must be the same type: varchar (1)
        Fix: select coalesce('1', cast(1 AS varchar))

        Args:
            sql (str): SQL to fix
            result (re.match): Regex match object

        Returns:
            str: Fixed SQL
        """
        line, column = int(result["line"]) - 1, int(result["column"]) - 1  # Presto to python array notation
        token, idx = self.ColumnCaster.get_problematic_token(sql, line, column)  # Should return an identifier list or identifier
        return sql[:idx] + f"cast({token} AS {result['type']})" + sql[idx + len(token.value):]

    def _case_statements(self, sql: str, result: re.match, **kwargs) -> str:
        """Casts all results in a case statement to the same data type.
        Case results need to have the same data type. The data type of the first branch is used.
        Example: select case when true then 'a' else 1 end --> line 1:37: All CASE results must be the same type: varchar (1)
        Fix: select case when true then 'a' else cast(1 AS varchar) end

        Args:
            sql (str): SQL to fix
            result (re.match): Regex match object

        Returns:
            str: Fixed SQL
        """
        line, column = int(result["line"]) - 1, int(result["column"]) - 1  # Presto to python array notation
        token, idx = self.ColumnCaster.get_problematic_token(sql, line, column)  # Should return an identifier list or identifier
        return sql[:idx] + f"cast({token} AS {result['type']})" + sql[idx + len(token.value):]

    def _cast_in_subquery(self, sql: str, result: re.match, **kwargs) -> str:
        """Value & subqueries results must have the same data type.
        Example: select 1 in (select '1') --> line 1:10: value and result of subquery must be of the same type for IN expression: integer vs varchar (1)
        Fix: select cast(1 AS varchar) in (select '1')

        Args:
            sql (str): SQL to fix
            result (re.match): Regex match object

        Returns:
            str: Fixed SQL
        """
        line, column = int(result["line"]) - 1, int(result["column"]) - 1  # Presto to python array notation
        token, idx = self.ColumnCaster.get_problematic_token(sql, line, column)  # Should return an identifier list or identifier
        logging.debug(f"[DEBUG] Found token {[token]}\nvalue:{token}|ttype:{token.ttype}\nparent:{token.parent}")
        return self.ColumnCaster.cast_non_trivial_tokens(sql, token, idx, result["f_type_0"], result.groupdict(), count_forward_tokens=0)

    def _cast_in(self, sql: str, result: re.match, **kwargs) -> str:
        """Value & IN statement must have the same type of data type.
        Example: select a \nfrom cte\nwhere a in (  \n 1, 2, 3) --> line 4:2: IN value and list items must be the same type: bigint (1)
        Fix: select a \nfrom cte\nwhere cast(a AS bigint) in (  \n 1, 2, 3)

        Args:
            sql (str): SQL to fix
            result (re.match): Regex match object

        Raises:
            ValueError: Data type in IN statement are inconsistent. It is not clear which data type
            should be chosen.

        Returns:
            str: Fixed SQL
        """
        line, column = int(result["line"]) - 1, int(result["column"]) - 1  # Presto to python array notation
        token, idx = self.ColumnCaster.get_problematic_token(sql, line, column)  # Should return an identifier list or identifier
        if isinstance(token, IdentifierList):
            token_in_list = [t for t in token.tokens if t.ttype not in (Whitespace, Punctuation, Comment, Newline)]
        else:
            token_in_list = [token]
        if all(t.ttype == Literal.String.Single for t in token_in_list):
            cast_to = "varchar"
        elif all(t.ttype == Literal.Number.Integer for t in token_in_list):
            cast_to = "bigint"
        elif all(t.ttype == Literal.Number.Float for t in token_in_list):
            cast_to = "double"
        else:
            raise ValueError(f"Inconsistent data type in the IN list! {[token_in_list]}")

        parent_idx = token.parent.tokens.index(token)
        for preceding_token in token.parent.tokens[parent_idx-1::-1]:  # Subtract the length of all preceding siblings
            idx -= len(preceding_token.value)
        grand_parent_idx = token.parent.parent.tokens.index(token.parent)  # Move up to the grand parent token. See tests to understand why this is relevant
        for preceding_token in token.parent.parent.tokens[grand_parent_idx-1::-1]:
            idx -= len(preceding_token.value)
            if preceding_token.ttype == Keyword and preceding_token.value.lower() == "in":
                token = preceding_token
                break
        return self.ColumnCaster.cast_non_trivial_tokens(sql, token, idx, cast_to, result.groupdict(), count_forward_tokens=0)

    def _cannot_cast_to_type(self, sql: str, result: re.match, **kwargs) -> str:
        """Try to fix some casting errors. This only support a narrow case: char(number)
        Example: select cast(a as integer) from cte --> line 1:8: Cannot cast char(10) to integer (1)
        Fix: select cast(trim(cast(a AS varchar)) AS integer) from cte

        Args:
            sql (str): SQL to fix
            result (re.match): Regex match object

        Returns:
            str: Fixed SQL
        """
        line, column = int(result["line"]) - 1, int(result["column"]) - 1  # Presto to python array notation
        token, idx = self.ColumnCaster.get_problematic_token(sql, line, column)
        if result["source_type"].lower().startswith("char"):  # char() brings issues to the table
            if "int" in result["target_type"].lower():  # char(X) -> tinyint/smallint/int(eger)/bigint
                replacement = Regex.sub(
                    r"cast\(\s*(?P<expression>.*)\s+as\s+(?P<data_type>\w+?)\s*\)",
                    lambda match: f"cast(trim(cast({match['expression']} AS varchar)) AS {match['data_type']})",
                    token.value
                )
                sql = sql[:idx] + replacement + sql[idx+len(token.value):]
        return sql  # No idea what to do, will return the same SQL and will fail again.

    def _between(self, sql: str, result: re.match, **kwargs) -> str:
        """Fixes data type mismatches in BETWEEN statements.
        Example: select a from cte where b between c and d --> line 1:27: Cannot check if varchar is BETWEEN varchar and date (1)
        Fix: select a from cte where b between c and cast(d AS varchar)

        Args:
            sql (str): SQL to fix
            result (re.match): Regex match object

        Returns:
            str: Fixed SQL
        """
        line, column = int(result["line"]) - 1, int(result["column"]) - 1  # Presto to python array notation
        token, idx = self.ColumnCaster.get_problematic_token(sql, line, column)
        return self.ColumnCaster.cast_non_trivial_tokens(sql, token, idx, "varchar", result.groupdict(), count_forward_tokens=3)

    def _table_not_found(self, sql: str, result: re.match, **kwargs) -> str:
        """For table names that start with "v", replace that first letter by "t" to go from "view" to "table"
        Example: select a from db.vcte --> Table 'db.vcte' not found (1)
        Fix: select a from db.tcte

        Args:
            sql (str): SQL to fix
            result (re.match): Regex match object

        Returns:
            str: Fixed SQL
        """
        table_instead_of_view = Regex.sub(
            r"(?P<database>\w+\.)?(?P<first_letter>\w)(?P<rest>\w*)",
            lambda match: match.groupdict()["database"]
            + ("t" if match.groupdict()["first_letter"] == "v" else match.groupdict()["first_letter"])
            + match.groupdict()["rest"],
            result["table"],
            strict=False
        )  # Change the first letter of the table name from "t" to "v"
        new_table_name = table_instead_of_view if table_instead_of_view != result["table"] else result["table"] + "_presto"
        return Regex.sub(
            r"{table}".format(table=re.escape(result["table"])),
            new_table_name,
            sql
        )  # strict as there must be at least one replacement

    def _unexpected_parameters(self, sql: str, result: re.match, **kwargs) -> str:
        """The current function got arguments of an incorrect data type. Only support concat function!!
        Example: select concat(1, '1') from b inner join c\n      ON a.my_col=b.another_col\nwhere d=e --> line 1:8: Unexpected parameters (bigint, varchar) for function concat (1)
        Fix: select concat(cast(1 AS varchar), cast('1' AS varchar)) from b inner join c\n      ON a.my_col=b.another_col\nwhere d=e

        Args:
            sql (str): SQL to fix
            result (re.match): Regex match object

        Raises:
            NotImplementedError: The function suffering that issue is not concat
            NotImplementedError: A single argument was fed to concat - that is not enough.

        Returns:
            str: Fixed SQL
        """
        if result["function_name"] == "concat":
            selected_sql = "\n".join(sql.split("\n")[int(result["line"])-1:])  # Capture all SQL starting at the issue
            selected_sql = selected_sql[int(result["column"])-1:]
            function_call = sqlparse.parse(selected_sql)[0].tokens[0]  # Extract the concat function call
            replacement = "concat"
            for token in function_call.tokens[1].tokens:  # Extract the building blocks
                print(f"token:{token}|{[token]}")
                if token.ttype in (Whitespace, Newline, Punctuation, Comment):
                    replacement += token.value
                elif isinstance(token, IdentifierList):  # Shallow replacement
                    for subtoken in token.tokens:
                        if subtoken.ttype in (Whitespace, Newline, Punctuation, Comment):
                            replacement += subtoken.value
                        else:
                            replacement += f"cast({subtoken.value} AS varchar)"
                else:  # Argument
                    raise NotImplementedError(f"You cannot have a single argument in a concat call. Also, only IdentifierList is supported right now.")
            return Regex.sub(
                r"{function_call}".format(function_call=re.escape(function_call.value)),
                replacement,
                sql
            )
        else:
            raise NotImplementedError(f"Parameter replacement of function {result['function_name']} is not yet supported.")

    def _cast_both_sides(self, sql: str, result: re.match, **kwargs) -> str:
        """Cast both sides of a comparison to varchar
        Example: select 'a' =1 --> line 1:12: '=' cannot be applied to varchar, bigint (1)
        Fix: select 'a' =cast(1 AS varchar)

        Args:
            sql (str): SQL to fix
            result (re.match): Regex match object

        Returns:
            str: Fixed SQL
        """
        line, column = int(result["line"]) - 1, int(result["column"]) - 1  # Presto to python array notation
        token, idx = self.ColumnCaster.get_problematic_token(sql, line, column)
        return self.ColumnCaster.cast_non_trivial_tokens(sql, token, idx, "varchar", result.groupdict())

    def _expand_wildcards(self, sql: str, temp_tgt_table_properties: Dict[str, str]) -> str:
        """Expand wildcards found in select statement to use the real column names. The column information
        is retrieved from Glue/describe formatted statement that was previously run
        Example: with cte AS (select b from cte2) select b from cte
        Replacement: with cte AS (select b from cte2) SELECT\nb\nfrom cte

        Args:
            sql (str): SQL to fix
            temp_tgt_table_properties (Dict[str, str]): List of known properties of the target table (from describe formatted call)

        Raises:
            ValueError: Found more than one Wildcard in the select statement

        Returns:
            str: Fixed SQL
        """
        is_distinct, span, final_select = utils.parse_final_select(sql)
        ddl_columns = [utils.format_column_name_presto(col) for col in temp_tgt_table_properties["columns"].keys()]  # Set of DDL columns
        wildcard_idx = None
        for idx, t in enumerate(final_select):
            if Regex.search(r"^(\w+\.)?\*$", t[0], strict=False):  # Only support one wildcard (the first encountered)
                if wildcard_idx is not None:  # Not "None" anymore
                    raise ValueError("Up to one wildcard is supported in select statements, but found a second one!")
                wildcard_idx = idx
            else:  # Before/after the wildcard (if any)
                matching_column_name = []
                if Regex.search(r"^(\w+\.)?(\w+|`.+?`)$", t[0], strict=False):  # Straight column name
                    matching_column_name.append(Regex.search(r"^(?P<prefix>\w+\.)?(?P<col_name>\w+|`.+?`)$", t[0]).groupdict()["col_name"])
                matching_column_name.append(t[1])
                ddl_columns = [
                    c
                    for c in ddl_columns
                    if c not in matching_column_name
                ]  # Filter out the matching column names found above

        if wildcard_idx is not None:  # A wildcard was found
            final_select_clean = final_select[:wildcard_idx] + [[col, None] for col in ddl_columns] + final_select[wildcard_idx+1:]
        else:  # No wildcard to expand!
            final_select_clean = final_select

        final_select_clean = [
            [c for c in column if c]  # Filter out the None when there is no alias
            for column in final_select_clean
        ]
        # Rebuild the final select statement (may or may not have been modified)
        sql = sql[:span[0]] \
            + "SELECT" + (" DISTINCT" if is_distinct else "") + "\n" \
            + ",\n".join([" AS ".join(n) for n in final_select_clean]) + "\n" \
            + sql[span[1]:]
        return sql

    def _column_type_mismatch(self, sql: str, result: re.match, **kwargs) -> str:
        """Blindly casts the column found to be of the wrong type by the SQL engine to the other type.
        This could be improved by collecting both types & having some logic to chose which side should be casted.

        Args:
            sql (str): SQL to fix
            result (re.match): Regex match object

        Returns:
            str: Fixed SQL
        """
        sql = self._expand_wildcards(sql, kwargs["temp_tgt_table_properties"])
        is_distinct, span, select_tokens = utils.parse_final_select(sql)
        print(f"Found select_tokens:{select_tokens}")
        # I. Try to find a match on the alias. This would have priority on anything else
        match_found = None
        for idx, column in enumerate(select_tokens):
            print(f"Processing:{column}")
            if column[1] == result["name"]:
                print(f"[Alias match] Found column {idx} to be a match on the alias:{column[1]} matched {result['name']}")
                match_found = idx
                break
        # II. Alias match did not work. Trying a expression match
        if match_found is None:
            print("Did not find an alias match. Parsing expressions")
            for idx, column in enumerate(select_tokens):
                print(f"Processing:{column}|match_found:{match_found}")
                parsed_expression = Regex.search(r"^(?P<prefix>\w+\.)?(?P<name>\w+|`.+?`)$", column[0], strict=False)
                is_column_match = parsed_expression.groupdict()["name"] if parsed_expression else None
                print(f"is_column_match:{is_column_match}")
                if is_column_match == result["name"]:
                    if match_found is not None:
                        print(f"[Expression match] Found another match at column {idx}. Exiting the column search")
                        match_found = None  # The column number match will be used
                        break
                    print(f"[Expression match] Found a column match on expression:{is_column_match} matched {result['name']}")
                    match_found = idx

        # III. If match_found from I. or II., use it. Otherwise rely on the column number.
        presto_column = match_found if match_found is not None else int(result['column']) - 1
        print(f"presto_column:{presto_column}")
        print(f"Casting entry {presto_column}:{select_tokens[presto_column]}")
        print(f"expr_type:{result['expr_type']}|ddl_type:{result['ddl_type']}")
        if "int" in result['expr_type'] and result['ddl_type'].startswith("char"):
            select_tokens[presto_column][0] = f"cast(cast({select_tokens[presto_column][0]} AS varchar) AS {result['ddl_type']})"
        else:
            select_tokens[presto_column][0] = f"cast({select_tokens[presto_column][0]} AS {result['ddl_type']})"
        if select_tokens[presto_column][1] is None:  # Re alias so now it's clear what column this is.
            select_tokens[presto_column][1] = result["name"]

        # IV. Eliminate the None aliases
        final_select_clean = [
            [c for c in column if c]  # Filter out the None when there is no alias
            for column in select_tokens
        ]

        # V. Rebuild the final select statement
        return sql[:span[0]] \
            + "SELECT" + (" DISTINCT" if is_distinct else "") + "\n" \
            + ",\n".join([" AS ".join(n) for n in final_select_clean]) + "\n" \
            + sql[span[1]:]

    def handle_errors(self, sql: str, original_sql: str, error_message: str, **kwargs) -> Tuple[str, str]:
        """General runner for handling validation errors sent back by Presto. The SQL is modified iteratively until
        there is no more errors, the error is not known or the exact same error came back twice in error despite a fix.

        Args:
            sql (str): SQL to fix
            original_sql (str): Hive SQL that is the baseline for the validation
            error_message (str): Error message sent back by Presto

        Raises:
            RuntimeError: Unknown issue: regex search against the list of known issue did not return a match.

        Returns:
            Tuple[str, str]: Fixed validation SQL (has validation tables in insert statement) & original SQL.
        """
        for known_issue in self.known_issues:
            result = Regex.search(known_issue, error_message, strict=False, case_sensitive=True)  # No guarantee to find known error
            if result:
                logging.debug(f"[DEBUG]Found match with result:{result}")
                sql = self.known_issues[known_issue](sql, result, **kwargs)
                if original_sql:
                    try:
                        original_sql = self.known_issues[known_issue](original_sql, result, **kwargs)
                    except Exception:
                        print(
                            "WARNING: Could not apply the same changes to the original SQL. "
                            "This can be due for a variety of reasons, like a parameter not having the same length as it's string representation."
                        )
                        original_sql = ""
                return sql, original_sql
        else:
            raise RuntimeError(f"This issue:\n{error_message}\nis not known. This will require to be manually fixed.")
