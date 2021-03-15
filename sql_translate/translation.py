import sqlparse
import re
import json
import os
import time
import collections
from termcolor import colored
import traceback
from typing import List, Tuple, Dict, Union, Optional
from sql_translate import utils
from sql_translate.engine import global_translation, recursive_translation, sql_format, regex

Regex = regex.Regex()


class _Translator():
    def __init__(self):
        pass


class HiveToPresto(_Translator):
    def __init__(
        self,
        output_extension: Optional[str] = None
    ) -> None:

        self.from_language = "Hive"
        self.to_language = "Presto"
        self.output_extension = output_extension if output_extension else self.to_language.lower()
        self.translation_stats = collections.defaultdict(int)

        # Instantiate each class
        self.Formatter = sql_format.Formatter()
        self.GlobalTranslator = global_translation.GlobalHiveToPresto()
        self.RecursiveTranslator = recursive_translation.RecursiveHiveToPresto()

    def _mask_tokens_that_are_note_hive_keywords(self, query: str) -> str:
        """Certain words are better processed masked so they are tagged as Identifier by sqlparse, not Keyword.
        The masking can be removed by _unmask_tokens_that_are_note_hive_keywords

        Args:
            query (str): Input SQL

        Returns:
            str: Masked SQL
        """
        return Regex.sub(
            r"\b({tokens_to_mask})\b".format(tokens_to_mask="|".join(utils.mask_everywhere)),
            lambda match: utils.mask + match.group(1),
            query,
            strict=False
        )

    def _unmask_tokens_that_are_note_hive_keywords(self, query: str) -> str:
        """Remove the Keyword masking done by _mask_tokens_that_are_note_hive_keywords

        Args:
            query (str): Input SQL

        Returns:
            str: Masked SQL
        """
        return Regex.sub(
            r"\b({tokens_to_unmask})\b".format(tokens_to_unmask="|".join([utils.mask + t for t in utils.mask_everywhere])),  # unmask
            lambda match: match.group(1).split(utils.mask)[1],
            query,
            strict=False
        )

    def _remove_over_shortcut(self, query: str) -> str:
        """OVER clauses are glued to the upcoming parenthesis to frame a Function to sqlparse.
        This function separates them again

        Args:
            query (str): Input SQL

        Returns:
            str: Cleared SQL
        """
        return Regex.sub(
            r"\bover\(",
            "OVER (",
            query,
            strict=False
        )

    def _remove_function_placeholders(self, query: str) -> str:
        """Remove the function placeholders if any

        Args:
            query (str): Input SQL

        Returns:
            str: Cleared SQL
        """
        return Regex.sub(
            r"\b{placeholder}\(".format(placeholder=utils.function_placeholder),
            "(",
            query,
            strict=False
        )

    def translate_statement(self, input_statement: str, has_insert_statement: bool = True, verbose: bool = False) -> str:
        """Translate a complete SQL statement (can have multiple queries in it)

        Args:
            input_statement (str): Input Hive SQL statement
            verbose (bool, optional): Increases verbosity. Defaults to False.

        Raises:
            NotImplementedError: Right now, only a single query per statement is supported
            Exception: Catches any kind of issue that could happen during the translation

        Returns:
            str: Translated SQL
        """
        self.translation_stats["Attempted"] += 1
        input_queries = [
            query
            for query in sqlparse.parse(input_statement)
            if query.value.strip()
        ]  # Remove empty input_queries
        if len(input_queries) > 1:
            raise NotImplementedError(f"The translation of more than 1 query per statement is not supported (found {len(input_queries)} queries)")
        try:
            final_translation = ""
            for input_query in input_queries:
                formatted_input_query = self.Formatter.format_query(input_query.value)
                # print(f"Formatted to: {formatted_input_query}")
                formatted_input_query = self._mask_tokens_that_are_note_hive_keywords(formatted_input_query)
                # print(f"Formatted & masked tokens: {formatted_input_query}")
                global_translation = self.GlobalTranslator.translate_query(formatted_input_query)
                # print(f"Global to: {global_translation}")
                recursive_translation = self.RecursiveTranslator.translate_query(global_translation, has_insert_statement=has_insert_statement)
                # print(f"Recursive to: {recursive_translation}")
                if has_insert_statement:  # If required & there isn't --> will raise. Not required and there is: validation will fail.
                    recursive_translation = self.GlobalTranslator.move_insert_statement(recursive_translation)
                # print(f"Final to: {final_translation}")
                final_translation = self._unmask_tokens_that_are_note_hive_keywords(recursive_translation)
                final_translation = self._remove_over_shortcut(final_translation)
                final_translation = self._remove_function_placeholders(final_translation)
                # print(f"Final clean to: {final_translation}")
        except Exception as err:
            self.translation_stats["Failed"] += 1
            msg = (
                f"ERROR: Failed translating query from {self.from_language} to {self.to_language} with message:\n{err}\n"
                f"Detailed traceback:\n{traceback.format_exc()}"
            )
            raise Exception(msg)
        else:
            self.translation_stats["Successful"] += 1
            return final_translation
        finally:
            if verbose:
                print(f"Statement translation statistics: {self.translation_stats}")

    def translate_file(self, path_file: str) -> str:
        """Translate a Hive SQL file to Presto SQL

        Args:
            path_file (str): Path to the file to translate

        Returns:
            str: Translated SQL
        """
        # Extract
        with open(path_file) as f:
            statement = f.read()
        # Transform
        translated_statement = self.translate_statement(statement)
        # Load
        *file_name, _ = os.path.basename(path_file).split('.')
        path_translated_file = os.path.join(os.path.dirname(path_file), '.'.join(file_name) + f".{self.output_extension}")
        with open(path_translated_file, 'w') as f:
            f.write(translated_statement)

        msg = (
            f"Successfully translated file '{os.path.basename(path_file)}' from {self.from_language} "
            f"to {self.to_language} and saved it to '{os.path.basename(path_translated_file)}'"
        )
        print(msg)
        return path_translated_file
