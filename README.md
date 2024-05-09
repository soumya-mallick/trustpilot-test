# Trustpilot take home assignment

The problem statement is broken down to two parts:
   1. Process to ingest csv file into a table.
   2. Process to enable crud operations on the table.

### Thought process

Identified two different components to perform these tasks independently. These components are designed to be resuable, scalable and extendable to other use cases.

### Design process

1. Database used is local installation of PostgreSQL.
2. Python3 is used as the language for development.
3. Designed two reusable components which tackles each part of the problem.
4. `file-ingestion` component is used to read a file and copy to a table in PostgreSQL. This is designed keeping in mind that it has scope to be extended to other file types or databases.
5. `crud-api` component handles the interaction by user and performs the crud operations.

### Assumptions and requirements

1. Availability of a PostgreSQL server either locally or hosted elsewhere.
2. Creation of a database in PostgreSQL using command - `CREATE DATABASE trustpilot;`
3. Provided dataset being in a .pdf file, it was converted to a csv file and column names were formatted to remove spaces.
4. `Email Address` field is assumed to be the primary key for this dataset for CRUD operations as it has the potential of being a unique identifier.
5. Availability of a config file at a location containing database credentials which can be used by the `file-ingestion` component. Currently, default values are provided to handle this.

### How to execute the file ingestion process?

To ingest, clean and load the CSV file data to the table:

1. Navigate to the path - `file-ingestion/src/`
2. Command - `python3 app.py -h` - provides information on what arguments the process takes.

Example usage:

```angular2html
python3 app.py csv /Documents/reviews_data.csv utf-8
```

### How to execute the crud-api process?

1. Navigate to path - `crud-api/src/`
2. Command - `python3 CrudApp.py -h` provides information on what arguments the process takes.
3. Providing the necessary arguments kicks off the workflow for any of the crud operations.

Example usage:
```angular2html
python3 CrudApp.py localhost trustpilot postgres admin reviews email_address
```

### Limitations of current process:

1. It doesn't expose a web interface for performing tasks. It currently is sort of a backend library meant for internal use through command line.
2. Select operation can filter only on one value for a given field.
3. Insert operation through `crud-api` can perform inserts one row at a time. This hinders bulk insert but the `file-ingestion` can help with this if data for bulk insert can be provided as a CSV file.
