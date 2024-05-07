import logging
import psycopg
import csv

class PostgreDB:
    def __init__(self, data, file_name, **kwargs):
        self.host = kwargs.get('host', 'localhost')
        self.database = kwargs.get('database','trustpilot')
        self.user = kwargs.get('user', 'postgres')
        self.data = data
        self.file_name = file_name
        self.conn = None

    def connect(self):
        self.conn = psycopg.connect(
            host=self.host,
            dbname=self.database,
            user=self.user
        )
        return self.conn


    def create_table(self):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS reviews(
                    reviewer_name text,
                    review_title text,
                    review_rating text,
                    review_content text,
                    email_address text PRIMARY KEY,
                    country text,
                    review_date text
                    )    
                    """)
                conn.commit()
                logging.info("Table created or verified successfully")

    def copy_to_table(self, csv_file_path):
        copy_cmd = "COPY reviews from STDIN WITH (FORMAT CSV, HEADER true)"
        try:
            with self.connect() as conn:
                with conn.cursor() as cur:
                    logging.info("Starting table copy operation")
                    with open(csv_file_path, 'r') as file:
                        with cur.copy(copy_cmd) as copy:
                            while data := file.read(100):
                                copy.write(data)
                    self.conn.commit()
                    logging.info("Successfully copied data to reviews table")
        except Exception as e:
            logging.error(f"Error occurred during PostgreSQL DB copy operation: {e}")
            conn.rollback()
        finally:
            conn.close()

    def write_data_to_csv(self):
        csv_file_name = f"{self.file_name.split('.')[0]}.csv".replace(" ", "_")
        file_path = csv_file_name
        if not self.data:
            raise ValueError("No clean data found to write to PostgreSQL DB")
        headers = self.data[0].keys() if self.data else []
        with open(csv_file_name, 'w') as csv_file:
            csv_writer = csv.DictWriter(csv_file, fieldnames=headers)
            csv_writer.writeheader()
            for line in self.data:
                csv_writer.writerow(line)
        csv_file.close()
        return file_path


    def process(self):
        self.create_table()
        csv_file_loc = self.write_data_to_csv()
        self.copy_to_table(csv_file_loc)








