import sqlparse
import os


class Formatter():
    def __init__(self, strip_comments: bool = True, keyword_case: str = "upper", identifier_case: str = "lower"):
        self.strip_comments = strip_comments
        self.keyword_case = keyword_case
        self.identifier_case = identifier_case

    def format_query(self, sql: str) -> str:
        """Format the query with sqlparse to harmonize the output formatting.
        Default formatting convention:
        - strip comments
        - upper case for keywords
        - lower case for identifiers

        Args:
            sql (str): Input SQL

        Returns:
            str: Formatted SQL.
        """
        sql = sqlparse.format(
            sql,
            strip_comments=self.strip_comments,
            keyword_case=self.keyword_case,
            identifier_case=self.identifier_case
        )
        return sql.strip().rstrip(";")  # Remove stray ; at the end.

    def format_file(self, path_file: str) -> str:
        """Format a given SQL file with sqlparse to harmonize the output formatting.
        Default formatting convention:
        - strip comments
        - upper case for keywords
        - lower case for identifiers

        Args:
            path_file (str): Path to the input file to translate

        Returns:
            str: Formatted SQL.
        """
        # Extract
        with open(path_file) as f:
            sql = f.read()
        # Transform
        formatted_sql = self.format_query(sql)
        # Load
        *file_name, extension = os.path.basename(path_file).split('.')
        path_formatted_file = os.path.join(os.path.dirname(path_file), '.'.join(file_name) + f"_formatted.{extension}")
        with open(path_formatted_file, 'w') as f:
            f.write(formatted_sql)

        print(f"Successfully formatted file '{os.path.basename(path_file)}' -> '{os.path.basename(path_formatted_file)}'")
        return path_formatted_file
