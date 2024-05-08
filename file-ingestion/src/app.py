import argparse
import logging
from importlib import import_module
import target.PostgreDB as PostgreDB

logging.basicConfig(level=logging.INFO)


def main():
    """ Main handler method to parse supplied arguments and perform table load operation"""
    parser = argparse.ArgumentParser(description="Read a CSV file and insert into Postgresql DB")
    parser.add_argument("filetype", type=str, help="File type")
    parser.add_argument("filepath", type=str, help="Path of the file")
    parser.add_argument("encoding", type=str, help="Type of file encoding")

    args = parser.parse_args()
    # Import appropriate file processing module and class based on provided file type
    file_module = import_module(f'source.{args.filetype.upper()}')
    file_class = getattr(file_module, args.filetype.upper())
    file_class_instance = file_class(args.filepath, args.encoding)

    results = file_class_instance.clean_data()

    file_name = args.filepath.split('/')[-1]
    postgreDB = PostgreDB.PostgreDB(results, file_name)
    postgreDB.process()


if __name__ == "__main__":
    main()
