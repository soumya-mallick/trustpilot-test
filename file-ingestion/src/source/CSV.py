import csv
import logging
import re

logging.basicConfig(level=logging.INFO)

class CSV:
    """ Handler for CSV files"""

    def __init__(self, file_path, file_encoding):
        self.file_path = file_path
        self.encoding = file_encoding

    def clean_data(self):
        rows_to_add = list()
        rejected_rows = list()
        with open(self.file_path, 'r', encoding=self.encoding) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                if self.is_valid_email(row['email_address']):
                    rows_to_add.append(row)
                else:
                    rejected_rows.append(row)

        logging.warning(f"{len(rejected_rows)} rows failed email validation check")
        logging.warning(f"Invalid emails: {rejected_rows}")
        return rows_to_add

    def is_valid_email(self, email):
        regex_pattern = r'^\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
        return re.match(regex_pattern, email) is not None


