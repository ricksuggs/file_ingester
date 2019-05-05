import csv
import logging
import sys
import traceback
from pathlib import Path

from app import db

data_path = Path("data")
specs_path = Path("specs")

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("file_ingester")
console = logging.StreamHandler()
logger.addHandler(console)


def ingest():

    logging.info("Starting ingestion")
    for child in data_path.iterdir():
        try:
            format_type, drop_date = parse_data_filename(child.name)

            logging.info(f"Processing data file: {child}")
            schema_filename = format_type + ".csv"

            schema = parse_schema_file(specs_path / schema_filename)
            rows = parse_data_file(child, schema)

            if len(schema) and len(rows):
                db.create_table(format_type, schema)
                db.insert(format_type, rows)

        except:
            log_error()
            continue


def parse_data_filename(data_filename):
    last_underscore_index = data_filename.rfind("_")
    if last_underscore_index == -1:
        raise Exception(f"Invalid data filename format: {data_filename}")
    format_type = data_filename[0:last_underscore_index]
    drop_date = data_filename[last_underscore_index:]
    return (format_type, drop_date)


def open_file(path):
    return open(path, newline=None)


def log_error():
    logging.error(f"Unexpected error: {sys.exc_info()[0]}")
    traceback.print_exc()


def parse_schema_file(schema_file_path):
    with open_file(schema_file_path) as spec_file:
        logging.info(f"Found schema file for data file: {schema_file_path}")
        schema = [row for row in csv.DictReader(spec_file)]
        return schema


def parse_data_file(data_file_path, schema):
    with open_file(data_file_path) as data_file:
        rows = extract_rows(data_file, schema)
        return rows


def extract_rows(data_file, schema):
    rows = []

    for line in data_file:
        row = {}
        line = line.rstrip()
        start = 0

        for schema_col in schema:
            end = start + int(schema_col["width"])
            try:
                if len(line) < end:
                    raise Exception(
                        f"Invalid row length encountered in data file: {line}"
                    )
                row[schema_col["column name"]] = convert(
                    line[start:end].rstrip(), schema_col["datatype"].strip()
                )
            except:
                log_error()
                break
            start = end
        # if an exception is raised parsing any line in the
        # data file, the entire data file is rejected
        else:
            rows.append(row)
            continue
        break
    return rows


def convert(value, data_type):
    if data_type == "BOOLEAN":
        return int(value)
    if data_type == "INTEGER":
        return int(value)
    if data_type == "TEXT":
        return value
    raise Exception("Unsupported data type encountered.")


if __name__ == "__main__":
    ingest()
