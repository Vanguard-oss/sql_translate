import sqlparse
import re
import json
from termcolor import colored
import os
import logging
from pathlib import Path
from typing import List, Dict, Tuple
from sqlparse.sql import IdentifierList, Identifier, Comparison, Where, Parenthesis, TokenList, Function, Case, Operation, SquareBrackets, TypedLiteral
from sqlparse.tokens import Keyword, DML, CTE, Whitespace, Newline, Punctuation, Number, Literal, Comment, Name, Operator, Wildcard, Token, Error
from sql_translate import utils
from sql_translate.engine import regex, special_functions_handling

# case insensitive wrapper enforcing that re methods actually have an impact
Regex = regex.Regex()
SpecialFunctionHandlerHiveToPresto = special_functions_handling.SpecialFunctionHandlerHiveToPresto()


class _RecursiveTranslator():
    def __init__(self):
        pass


class RecursiveHiveToPresto(_RecursiveTranslator):
    def __init__(self):
        self.from_language = "Hive"
        self.to_language = "Presto"
        self.allowable_sqlparse_ttypes = {
            "varchar": [Literal.String.Single, Literal.String.Symbol],
            "bigint": [Number.Integer],
            "double": [Number.Float],
            "timestamp": [],
            "date": [],
            "any": []
        }
        self.regex_hive_insert = utils.regex_hive_insert

        # Grab all sub dictionaries and merge them into one
        self.functions = {}
        path_files = Path(os.path.join(os.path.dirname(__file__), "..", "function_dictionaries", "hive_to_presto")).rglob("*.json")
        for path_file in path_files:
            with open(path_file) as f:
                self.functions = {**self.functions, **json.load(f)}  # Only load once at object creation

    def translate_query(self, query: str, has_insert_statement: bool = True) -> str:
        """Translate an entire query from Hive to Presto

        Args:
            query (str): Input query
            has_insert_statement (bool, optional): Flag indicating the presence of an insert statement in the query. Defaults to True.

        Raises:
            ValueError: A statement was passed, not a query (a statement can have multiple queries)

        Returns:
            str: Translated query (to Presto)
        """
        # Reset key parameters
        if has_insert_statement:
            _, _, _, self.partition_info = utils.parse_hive_insertion(query)
        else:
            self.partition_info = {}
        # Breakdown the query
        query = sqlparse.parse(query)  # Assumes that a query, not a statement, was passed
        if len(query) > 1:
            raise ValueError(f"A statement was passed for recursive translation, containing multiple queries.")
        return self._breakdown_query(query[0].tokens, verbose=False)

    def _breakdown_parenthesis(self, parenthesis_token: Parenthesis) -> List:
        """Helper function extracting the content of a Parenthesis sqlparse token.
        Essentially, removes the start & end punctuation.

        Args:
            parenthesis_token (Parenthesis): Input parenthesis token

        Returns:
            List: tokens inside the parenthesis
        """
        # Unpack parenthesis into its own tokens & return them
        # Special case: void function
        if parenthesis_token.value == "()":
            return []

        # Arguments are present: either a single Identifier or an IdentifierList
        parenthesis_content = []
        for token in parenthesis_token.tokens[1:-1]:
            parenthesis_content += token.tokens if isinstance(token, IdentifierList) else [token]

        return parenthesis_content

    def _translate_function(self, tokens: List, verbose=False) -> Tuple[str, str]:  # A function call has been identified. Could be nested
        """Translates a Hive function to its Presto equivalent.
        This is by far the most important function of the whole module. Unfortunately, it is a little longer than hoped and could benefit
        from being broken down into smaller functions (or maybe moved into its own object).
        This function is recursive, meaning that it will parse the arguments & call itself if another function is found.
        For instance: count(concat(a, b)) would start with count & call itself to translate concat too.
        There are a few steps in the translate_function process. These are indicated with inline comments in the source code.

        Args:
            tokens (List): List of argument tokens for the function
            verbose (bool, optional): Increases the verbosity. Defaults to False.

        Raises:
            NotImplementedError: Function signature cannot be found in the function_dictionaries folder. Might need to add it.
            IndexError: Not enough arguments provided for the function based on its definition in function_dictionaries
            Exception: Should not happen - this exception means a new type of sqlparse operation (like Comparison, Operation, ...) was found and not supported.
            NotImplementedError: Functions with 'unlimited' input_type can only accept one argument!
            IndexError: A required argument was missing
            ValueError: Corner case error when trying to parse a number as a string or vice versa

        Returns:
            Tuple[str, str]: Returns the translated function & its output data type (per function_dictionaries entry)
        """
        # I. Special cases
        # For instance, function becomes keyword eg current_date() -> current_date
        function_name = tokens[0].value.lower()
        try:
            if "signature" not in self.functions[function_name]:  # function -> keyword translation, eg current_date() -> current_date
                return self.functions[function_name]["translation"], self.functions[function_name]["returns"]
        except Exception as err:
            msg = (
                f"Failed with message: {err}. Signature cannot be found for {function_name}."
                f"Is {function_name} defined at all?"
            )
            raise NotImplementedError(msg)

        nb_required_arguments = sum([
            True
            for argument in self.functions[function_name].get("signature", [])  # Could become a keyword and not have a signature key
            if isinstance(argument["input_argument_nb"], int) and not argument.get("optional", False)  # Discard
        ])  # Count only the required arguments

        # Watch out: 'count   \n(a)' is parsed as a function. The formatting should be preserved.
        function_name_formatting = "".join([tokens[idx].value for idx in range(1, len(tokens)-1)])  # Grabs '   \n' from previous example
        assert isinstance(tokens[-1], Parenthesis)  # Sanity check: a function is always parsed with complete parenthesis as last token
        tokens = [tokens[0]] + self._breakdown_parenthesis(tokens[-1])  # Ignore function name formatting
        if len(tokens) == 1:  # There was nothing inside the parenthesis
            if nb_required_arguments == 0:  # All good: no required args were expected (maybe some optional arguments)
                if function_name == "unix_timestamp":  # SPECIAL CASE
                    return "cast(to_unixtime(cast(current_timestamp AS timestamp)) AS bigint)", self.functions[function_name]["returns"]
                else:
                    return self.functions[function_name]["translation"] + "()", self.functions[function_name]["returns"]
            else:  # At least one required argument should have been provided!
                msg = (
                    f'ERROR: \'{self.functions[function_name]["translation"]}\', the corresponding {self.to_language} function for \'{function_name}\' '
                    f'takes {nb_required_arguments} required argument(s) but none were provided.'
                )
                raise IndexError(msg)

        # Main loop: iterate over all arguments. If one is a function, recursive call.
        # The goal is to eventually a string representing the translated function call

        # II. Extract all the arguments
        # II.1. Create input_arguments that contains all arguments as {"value": token.value, "type": token.ttype}
        input_arguments = []
        is_distinct = False  # Tracks if the function arguments are called with the distinct modifier
        is_first_argument = True
        logging.debug(f"[DEBUG][{function_name}] Tokens inside function:{tokens[1:]}")

        def get_tokens_before_as(tokens) -> List[str]:
            expression_tokens = []
            for t in tokens:  # Last token would be after AS or would start with over(
                if t.ttype != Keyword and t.value.lower() != "as":
                    expression_tokens.append(t.value)
                else:
                    return expression_tokens
            return expression_tokens  # There was no AS token
        if function_name == "cast":
            logging.debug(f"[DEBUG][{function_name}] Found a cast function! tokens:{tokens}")
            expression = ""
            for argument_token in tokens[1:]:
                if argument_token.ttype in (Whitespace, Newline, Punctuation, Comment):
                    expression += argument_token.value
                    continue  # Do not increment argument position
                if Regex.search(r"\bover\(", argument_token.value, strict=False):  # Embedded window function (starts with over)
                    logging.debug(f"[DEBUG][{function_name}] {argument_token} contains an OVER clause!")
                    expression = "".join(get_tokens_before_as(argument_token.tokens[:-1]))  # Anything but the end
                    expression += "".join(get_tokens_before_as(argument_token.tokens[-1].tokens))  # Extract data type from last token
                    cast_target_type = argument_token.tokens[-1].tokens[-1].value
                else:  # No window function
                    expression = "".join(get_tokens_before_as(argument_token.tokens))
                    cast_target_type = argument_token.tokens[-1].value  # Actual type can be decoded as Builtin or Function but only the value matters
            expression = expression.rstrip()
            logging.debug(f"[DEBUG][{function_name}] Found expression:{expression}")
            logging.debug(f"[DEBUG][{function_name}] Found cast_target_type:{cast_target_type}")
            argument_tokens = sqlparse.parse(expression)[0].tokens  # Recreate all tokens without the data type
        else:  # No change
            argument_tokens = tokens[1:]  # Skip function name
        logging.debug(f"[DEBUG][{function_name}] argument_tokens:{argument_tokens}")

        for token in argument_tokens:
            if token.ttype in (Whitespace, Newline, Punctuation, Comment):
                continue  # Do not increment argument position
            elif isinstance(token, SquareBrackets):  # Exceptional case...do not increment count
                input_arguments[-1]["value"] += token.value  # Combine the content with the previous entry
            else:  # Not a formatting consideration
                if is_first_argument and token.ttype == Keyword and token.value.lower() == "distinct":
                    is_distinct = True
                else:
                    # Process the argument
                    if hasattr(token, "tokens"):  # The token can be broken down further
                        if isinstance(token, Function):  # Function to translate
                            value, output_type = self._translate_function(token.tokens, verbose=verbose)
                            input_arguments.append({"value": value, "type": output_type})
                        elif isinstance(token, Identifier):
                            input_arguments.append({"value": token.value, "type": token.ttype})
                        elif isinstance(token, TypedLiteral):  # Stuff like TIMESTAMP '9999-12-31'
                            input_arguments.append({"value": token.value, "type": token.ttype})
                        elif type(token) in (Parenthesis, Case, Comparison, Operation):
                            input_arguments.append({"value": self._breakdown_query(token.tokens), "type": token.ttype})
                        else:  # A new type of argument has been found. Modify this source code to take it into account.
                            raise Exception(
                                f"[DEBUG] ERROR! Encountered an unknown argument type while breaking down a Function. Details:\n"
                                f"token:{[token]}|token:{token}|ttype:{token.ttype}"
                            )
                    else:  # Not a function as an
                        input_arguments.append({"value": token.value, "type": token.ttype})  # ttype is Identifier, Name, ...

                is_first_argument = False
        logging.debug(f"[DEBUG][{function_name}] input_arguments:{input_arguments}")

        # II. 2. Handle boolean operators that break the argument list
        input_arguments_clean = []
        while input_arguments:
            argument = input_arguments.pop(0)
            if Regex.search(r"^(or|and)$", argument["value"], strict=False):
                logging.debug(f"[DEBUG][{function_name}] Found or/and:{argument['value']}")
                next_argument = input_arguments.pop(0)  # Get the next argument
                input_arguments_clean[-1]["value"] += f' {argument["value"]} {next_argument["value"]}'  # Append to last argument
            elif Regex.search(r"^(over\()", argument["value"], strict=False):  # OVER needed for embedded window functions...but could be an alias...
                logging.debug(f"[DEBUG][{function_name}] Found over:{argument['value']}")
                input_arguments_clean[-1]["value"] += f' {argument["value"]}'  # Append to last argument
            else:
                input_arguments_clean.append(argument)
        logging.debug(f"[DEBUG][{function_name}] input_arguments_clean:{input_arguments_clean}")

        # III. Swap, discard & add arguments. Creates swapped_arguments
        swapped_arguments = []  # The swap could be the indentity function (no change of position)
        signature = self.functions[function_name]["signature"]
        if any([argument["input_argument_nb"] == "unlimited" for argument in signature]):
            try:
                assert len(signature) == 1
            except:
                raise NotImplementedError(f"Functions with 'unlimited' input_type can only accept one argument!")
        for output_argument in signature:
            if output_argument["input_argument_nb"] == "unlimited":
                swapped_arguments = [arguments for arguments in input_arguments_clean]  # Simple copy, signature has single argument for now
            elif isinstance(output_argument["input_argument_nb"], str) and output_argument["input_argument_nb"] != "unlimited":  # Static argument to be added
                swapped_arguments.append({"value": output_argument["input_argument_nb"], "type": output_argument["data_type"]})
            else:  # Link to an input_argument
                try:
                    swapped_arguments.append(input_arguments_clean[output_argument["input_argument_nb"]])
                except IndexError:
                    if output_argument.get("optional"):  # Argument cannot be found but it was optional
                        continue
                    raise IndexError(f'An input position argument is missing to translate {function_name} -> {self.functions[function_name]["translation"]}!')
        logging.debug(f"[DEBUG][{function_name}] swapped_arguments:{swapped_arguments}")
        # logging.debug(f"[DEBUG] signature {signature}")

        # IV. Cast to desired data type. Creates str_output_arguments
        str_output_arguments = []
        for output_idx, argument in enumerate(swapped_arguments):
            # Define key parameters required_output_type & output_arg_str_value
            if signature[0]["input_argument_nb"] == "unlimited":
                output_idx = 0  # Functions with 'unlimited' input_type can only accept one argument!
            required_output_type = signature[output_idx]["data_type"]
            output_arg_str_value = argument["value"]

            # Append final string value, surrounded or not by cast functions.
            if argument["type"] in (required_output_type, *self.allowable_sqlparse_ttypes[required_output_type]) or required_output_type == "any":
                # print(f"No casting for {output_arg_str_value}")
                str_output_arguments.append(output_arg_str_value)  # The output type is already correct!
            else:  # Casting might be required. Let's rule out all known cases before forcing a cast
                # 1. varchar -> numbers can potentially be handled without calling 'cast'
                if argument["type"] in self.allowable_sqlparse_ttypes["varchar"] and required_output_type in ("bigint", "double"):
                    # The input is a varchar while a numeral (bigint or double) is required, eg '5' when a bigint is required.
                    try:
                        str_output_arguments.append(utils.char_to_number(output_arg_str_value, required_output_type))
                    except Exception:
                        msg = (
                            f'ERROR: An unknown input {output_arg_str_value} (type {argument["type"]}) was provided instead of {required_output_type}.'
                            f" It could not be parsed by python as a {required_output_type} either."
                        )
                        raise ValueError(msg)
                # 2. numbers -> varchar can always be handled without calling 'cast'
                elif argument["type"] in self.allowable_sqlparse_ttypes["bigint"] + self.allowable_sqlparse_ttypes["double"] and required_output_type == "varchar":
                    # Numeral that needs to be a varchar -> just surround with single quotes
                    str_output_arguments.append(f'\'{output_arg_str_value}\'')
                # 3. Everything else needs to be explicitly casted!
                else:
                    # logging.debug(f"[DEBUG] required_output_type: {required_output_type}")
                    if required_output_type == "date":  # Special case: cast to timestamp, then to date
                        # print(f'[DEBUG] Found a date required! {output_arg_str_value}')
                        str_output_arguments.append(f'cast(cast({output_arg_str_value} AS timestamp) AS date)')
                    else:
                        str_output_arguments.append(f'cast({output_arg_str_value} AS {required_output_type})')
        logging.debug(f"[DEBUG][{function_name}] str_output_arguments:{str_output_arguments}")

        # V. Final translation: apply any compositions to arguments & join. Creates str_output_arguments_composed
        if self.functions[function_name].get("compositions"):
            str_output_arguments_composed = self._apply_compositions(str_output_arguments, self.functions[function_name]["compositions"])
        else:
            str_output_arguments_composed = str_output_arguments
        logging.debug(f"[DEBUG][{function_name}] str_output_arguments_composed:{str_output_arguments_composed}")

        # VI. Special functions
        # These are functions that require some special treatment that the previous steps could not provide.
        # They are defined in a separate module.
        logging.debug(f"[DEBUG][{function_name}] Special functions processing")
        if is_distinct:  # Introduce the distinct back in the 1st argument
            str_output_arguments_composed[0] = "distinct " + str_output_arguments_composed[0]

        if function_name == "cast":
            cast_target_type = "varchar" if cast_target_type.lower() == "string" else cast_target_type.lower()
            translated_function_call = self.functions[function_name]["translation"] + function_name_formatting + "(" + ", ".join(str_output_arguments_composed) + f" AS {cast_target_type})"
        elif function_name != "cast" and self.functions[function_name]["translation"] == "cast":  # Conversion to cast call - rare but corner case
            translated_function_call = "cast" + function_name_formatting + "(" + ", ".join(str_output_arguments_composed) + f' AS {self.functions[function_name]["returns"]})'
        elif SpecialFunctionHandlerHiveToPresto.special_functions.get(function_name):  # Special function management
            translated_function_call = SpecialFunctionHandlerHiveToPresto.special_functions[function_name](
                self.functions[function_name],
                function_name_formatting,
                input_arguments_clean,
                str_output_arguments_composed
            )
        else:
            translated_function_call = self.functions[function_name]["translation"] + function_name_formatting + "(" + ", ".join(str_output_arguments_composed) + ")"

        logging.debug(f"[DEBUG][{function_name}] translated_function_call:{translated_function_call}")

        # VII. Return final translation
        if function_name == "cast":  # Read what type it casted to
            return translated_function_call, cast_target_type
        else:
            return translated_function_call, self.functions[function_name]["returns"]

    def _apply_compositions(self, str_output_arguments: List, compositions: List[Dict]) -> List:
        """Compositions allow you to manipulate arguments in creative ways to adjust for the difference in signatures.

        Args:
            str_output_arguments (List): List of arguments for the translated function
            compositions (List[Dict]): Mathematical/logical operations to perform on them

        Raises:
            AssertionError: Raised when sanity checks fail

        Returns:
            List: Processed arguments
        """
        # Sanity checks & expand "end" keyword
        for composition in compositions:
            try:
                if isinstance(composition["args"], list):
                    assert composition["args"]  # Cannot be empty
                    if len(composition["args"]) == 2:
                        if isinstance(composition["args"][1], str):  # Expand "end"
                            assert composition["args"][1] == "end"
                            composition["args"] = list(range(composition["args"][0], len(str_output_arguments)))
                    assert composition["args"] == list(range(composition["args"][0], composition["args"][-1]+1))  # Continuity
                else:  # If not a list, can only be the "all" keyword
                    assert composition["args"] == "all"
            except AssertionError:
                raise AssertionError(f"There are one or more errors with these composition arguments: {composition}")

        # Process compositions
        # 'arg' is the argument under consideration. 'args' is all the arguments (so a specific one can be accessed).
        for idx, composition in enumerate(compositions):
            if composition["args"] == "all":
                if composition.get("as_group"):  # Outputs str, encapsulate in list for further processing
                    # print(f"all as group: {', '.join(str_output_arguments)}")
                    str_output_arguments = [composition["formula"].format(arg=", ".join(str_output_arguments), args=str_output_arguments)]
                elif composition.get("merged"):
                    str_output_arguments = [composition["formula"].format(arg=" ".join(str_output_arguments), args=str_output_arguments)]
                else:
                    str_output_arguments = [composition["formula"].format(arg=arg, args=str_output_arguments) for arg in str_output_arguments]
            else:  # List of continuous indexes
                if composition.get("as_group"):
                    str_output_arguments = str_output_arguments[:composition["args"][0]] + \
                        [composition["formula"].format(arg=", ".join(str_output_arguments[composition["args"][0]:composition["args"][-1]+1]), args=str_output_arguments)] + \
                        str_output_arguments[composition["args"][-1]:]
                elif composition.get("merged"):
                    str_output_arguments = str_output_arguments[:composition["args"][0]] + \
                        [composition["formula"].format(arg=" ".join(str_output_arguments[composition["args"][0]:composition["args"][-1]+1]), args=str_output_arguments)] + \
                        str_output_arguments[composition["args"][-1]:]
                else:  # Apply composition to individual entries in str_output_arguments if index in composition["args"]. Otherwise, just copy.
                    str_output_arguments = [
                        composition["formula"].format(arg=str_output_arguments[idx], args=str_output_arguments) if idx in composition["args"] else str_output_arguments[idx]
                        for idx in range(len(str_output_arguments))
                    ]
            str_output_arguments = [_ for _ in str_output_arguments if _]  # Clear "" entries from the list. Allows to drop arguments.
        return str_output_arguments

    def _breakdown_query(self, tokens: List, verbose=False) -> str:
        """Top level recursive function that breaks down the query into bits that can be translated.
        Most of the tokens coming out of sqlparse are copy/pasted. Functions are treated separately via the _translate_function recursive method.

        Args:
            tokens (List): List of tokens to translate
            verbose (bool, optional): Increases verbosity. Defaults to False.

        Raises:
            Exception: Catches any kind of issue that could happen during a recursive call

        Returns:
            str: Translated SQL
        """
        translated_query = ""
        for token in tokens:
            if token.ttype in (Whitespace, Newline, Punctuation, Comment, Name):
                translated_query += token.value
            elif token.ttype in (
                    Token.Name.Builtin,
                    Keyword, Keyword.DML, Keyword.CTE, Keyword.Order,
                    Operator, Operator.Comparison,
                    Literal.String.Single, Literal.String.Symbol, Literal.Number.Integer, Literal.Number.Float,
                    Wildcard,
                    Error):
                translated_query += token.value
            elif isinstance(token, Function):  # Enter top level function call
                # print(f"Found: {token} that contains {token.tokens}")
                translation, _ = self._translate_function(token.tokens, verbose=verbose)
                translated_query += translation
            else:  # Should be always possible to break this token into tokens
                try:
                    translated_query += self._breakdown_query(token.tokens, verbose=verbose)
                except Exception as err:
                    print(colored(f"[DEBUG] Could not break a token into more pieces. Reason:\"{err}\"\ntoken:{[token]}\ntoken_content:{token.value}\nttype:{token.ttype}", "red", attrs=["bold"]))
                    raise Exception
        return translated_query
