import re
from typing import Union
import logging


class Regex():
    """Regex is a custom re wrapper for the purpose of SQL translation.
    It enforces that calls to re methods actually do something. Otherwise, exceptions are raised.
    """

    def __init__(self) -> None:
        self.msg = [
            "Call to re.{name} with pattern:/{pattern}/ {error_msg}.",
            "This is not acceptable behavior. Input:\n{string}\n"
        ]
        self.blacklist = (
            r"^\d",  # Used to check if an identifier starts with a digit. Generate tons of noise.
            r"(?P<utf8>%[\dA-F]{2})"  # Used to replace corrupter utf8 characters by their true value
        )

    def _unexpected_behavior(self, name: str, pattern: str, error_msg: str, string: str, strict: bool) -> None:
        """Helper function surfacing information about the regex failure.

        Args:
            name (str): Name of the re method that failed
            pattern (str): Pattern that was used
            error_msg (str): Custom error message
            string (str): String that was being acted on
            strict (bool): value of the strict kwarg

        Raises:
            Exception: If strict was True, an exception is raised. Otherwise, a warning is issued.
        """
        if strict:
            raise Exception(" ".join(self.msg).format(name=name, pattern=pattern, error_msg=error_msg, string=string))
        else:
            if pattern not in self.blacklist:
                logging.warning(self.msg[0].format(name=name, pattern=pattern, error_msg=error_msg, string=string))

    def search(self, pattern: str, string: str, strict: bool = True, case_sensitive: bool = False) -> re.match:
        """Wrapper for re.search
        Enables to set two additional parameters so that an Exception is raised if the regex search fails.
        1. strict
        2. case_sensitive

        Args:
            pattern (str): regex pattern
            string (str): string to search
            strict (bool, optional): if the strict flag is set and re.search returns None, an exception will be raised by _unexpected_behavior. Defaults to True.
            case_sensitive (bool, optional): if the case_sensitive flag is set, re.search is run without the re.IGNORECASE flag. Defaults to False.

        Returns:
            re.match: output of the re.search call
        """
        if case_sensitive:
            result = re.search(pattern, string)
        else:
            result = re.search(pattern, string, flags=re.IGNORECASE)
        if result:  # A match object was returned!
            return result
        self._unexpected_behavior("search", pattern, "returned no result", string, strict)

    def sub(self, pattern: str, repl: Union[str, callable], string: str, strict: bool = True, case_sensitive: bool = False) -> str:
        """Wrapper for re.sub
        Enables to set two additional parameters so that an Exception is raised if the regex search fails.
        1. strict
        2. case_sensitive

        Args:
            pattern (str): regex pattern
            repl (Union[str, callable]): replacement string
            string (str): string to search
            strict (bool, optional): if the strict flag is set and re.search returns None, an exception will be raised by _unexpected_behavior. Defaults to True.
            case_sensitive (bool, optional): if the case_sensitive flag is set, re.search is run without the re.IGNORECASE flag. Defaults to False.

        Returns:
            re.match: output of the re.search call
        """
        if case_sensitive:
            result = re.sub(pattern, repl, string)
        else:
            result = re.sub(pattern, repl, string, flags=re.IGNORECASE)
        if result == string:  # No substitution happened!
            self._unexpected_behavior("sub", pattern, "did not yield any substitutions", string, strict)
        return result  # Returns only if sub happened or non strict behavior
