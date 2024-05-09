import logging
import argparse
import re
from DBTable import DBTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CrudApp:
    """ Handler class to process each kind of crud request """

    def __init__(self, host, database, user, password, table, primary_key):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.table = table
        self.primary_key = primary_key
        self.table = DBTable(self.host, self.database, self.user, self.password, self.table, self.primary_key)

    @staticmethod
    def is_valid_email(email):
        """
        Performs email validation.
        :param email: email address
        :return: return True if email matches the regex, otherwise False
        """
        regex_pattern = r'^\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
        if re.match(regex_pattern, email):
            return True
        return False

    def process_request(self):
        """ Handler method to perform crud operations based on inputted operation type """
        table_op_type = input("Enter the operation type i.e. select, update, insert, delete: ").strip().lower()
        if table_op_type == "select":
            self._handle_select()
        elif table_op_type == "insert":
            self._handle_insert()
        elif table_op_type == "update":
            self._handle_update()
        elif table_op_type == "delete":
            self._handle_delete()

    def _handle_select(self):
        """ Handles select operation and prints results to console """
        fields = input("Enter columns to select separated by commas(or leave blank to select all): ").strip().lower()
        field_list = [field.strip() for field in fields.split(',')] if fields else ['*']
        if field_list:
            filter_field = input("Enter field name to filter by(or leave blank for no filter): ").strip()
            if filter_field:
                filter_value = input(f"Enter value for {filter_field}: ")
                logging.info(f"Selecting data with condition {filter_field} = {filter_value}")
                print(self.table.select(field_list, filter_field, filter_value))
            else:
                logging.info("Selecting data with no filter applied")
                logger.info(self.table.select(field_list))

    def _handle_insert(self):
        """ Handles insert operation and prints inserted record only to console """
        logger.info("Enter field, value pairs to insert(leave blank when done).")
        field_value_dict = dict()
        while True:
            field = input("Enter field to insert: ").strip()
            if field == '':
                break
            else:
                field_value = input(f"Enter value for {field}: ")
                if field == "email_address":
                    while not self.is_valid_email(field_value):
                        logger.warning("Invalid value for email address. Please enter valid value.")
                        field_value = input(f"Enter value for {field}: ")
                field_value_dict[field] = field_value

        if field_value_dict:
            logger.info("Inserted record: ")
            logger.info(self.table.insert(field_value_dict))
        else:
            logger.info("No data provided for insert operation")

    def _handle_update(self):
        """ Handles update operation """
        primary_key_val = input(f"Enter {self.primary_key} value for record to update: ")
        logger.info("Enter field, value pairs to update(leave blank when done).")
        field_value_dict = dict()
        while True:
            field = input("Enter field to update: ").strip()
            if field == '':
                break
            else:
                field_value = input(f"Enter value for {field}: ")
                field_value_dict[field] = field_value

        if field_value_dict and primary_key_val:
            logger.info("Record updated")
            logger.info(self.table.update(field_value_dict, primary_key_val))
        else:
            logger.info("No data provided for update operation")

    def _handle_delete(self):
        """ Handles delete operation """
        print("Choose your option")
        print("1: Delete specific records")
        print("2: Delete all records")
        choice = input("Provide your input: ")
        if choice == "1":
            field = input("Enter field name for where clause condition: ").strip()
            field_value = input(f"Enter value for {field}: ").strip()
            if field and field_value:
                confirm = input(f"Do you want to delete records where {field} = {field_value}? (yes/no): ").lower()
                if confirm == "yes":
                    logger.info("Deleted record: ")
                    logger.info(self.table.delete(field, field_value))
                else:
                    logger.info("Delete operation cancelled")
            else:
                logger.warning("No input provided for conditional delete")
        elif choice == "2":
            self.table.delete()
        else:
            logger.warning("Invalid choice")


def main():
    """ Main method to initiate database connection and execute crud operations """
    parser = argparse.ArgumentParser(description="Performs CRUD operations on a table available in Postgresql DB")
    parser.add_argument("host", type=str, help="Host Name")
    parser.add_argument("database", type=str, help="Database")
    parser.add_argument("user", type=str, help="User")
    parser.add_argument("password", type=str, help="password")
    parser.add_argument("table", type=str, help="Table")
    parser.add_argument("primary_key", type=str, help="Primary Key")

    args = parser.parse_args()

    crud = CrudApp(args.host, args.database, args.user, args.password, args.table, args.primary_key)
    crud.process_request()


if __name__ == "__main__":
    main()
