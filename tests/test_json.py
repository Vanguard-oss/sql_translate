import jsonschema
import unittest
import pytest
from pathlib import Path
import os
import json

# Load all JSON files in RAM
path_function_dictionaries = os.path.join(
    os.path.dirname(__file__), "..", "sql_translate", "function_dictionaries"
)
path_jsonschema = os.path.join(
    os.path.dirname(__file__), "samples", "jsonschema", "function_dictionaries.json"
)
files_to_review = {}
for path_file in Path(os.path.join(path_function_dictionaries)).rglob("*.json"):
    print(f"Loading {path_file} in memory")
    with open(path_file) as f:
        files_to_review[path_file] = json.load(f)
assert files_to_review  # If empty list, file must have moved


def test_dictionaries_against_jsonschema() -> None:
    with open(path_jsonschema) as f:
        schema = json.load(f)
    for file_to_review, content in files_to_review.items():
        print(f"Validating {file_to_review} against jsonschema")
        assert jsonschema.validate(content, schema) is None


def test_dictionaries_are_sorted() -> None:
    for file_to_review, content in files_to_review.items():
        print(f"Validating {file_to_review} has sorted keys")
        assert list(content.keys()) == sorted(content.keys())
