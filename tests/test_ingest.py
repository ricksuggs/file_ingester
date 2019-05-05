import csv
from pathlib import Path

from app import ingest


def test_extract_rows():
    path = Path(__file__).parent
    with open(path / "test_data/specs/test1.csv", newline=None) as spec_file, open(
        path / "test_data/data/test1_2019-05-05.txt"
    ) as data_file:

        schema = [row for row in csv.DictReader(spec_file)]

        rows = ingest.extract_rows(data_file, schema)

        assert len(rows) == 3

def test_extract_rows_invalid_row_length():

    # if the data file has an invalid row length
    # then the entire data file will be rejected
    # and the rows result should be empty
    path = Path(__file__).parent
    with open(path / "test_data/specs/test1.csv", newline=None) as spec_file, open(
        path / "test_data/data/test1_2019-05-06.txt"
    ) as data_file:

        schema = [row for row in csv.DictReader(spec_file)]

        rows = ingest.extract_rows(data_file, schema)

        assert len(rows) == 0
