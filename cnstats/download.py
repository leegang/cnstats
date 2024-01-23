import csv
import logging
import os
import random
import time
from datetime import datetime
import duckdb
import re
from cnstats.stats import stats
from .utils import next_month, previous_month

# Constants
LAST_MONTH = previous_month(datetime.today().strftime("%Y%m"))
PATTERN = r"\((\d{4})?-(\d{4}|至今)?\)$"
DUCKDB_FILE = "data/macrodb.duckdb"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [line:%(lineno)d] - %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

class DataDownloader:
    def __init__(self, dbcode, dbtype, download_code_csv, start_datestr, end_datestr):
        self.dbcode = dbcode
        self.dbtype = dbtype
        self.download_code_csv = download_code_csv
        self.start_datestr = start_datestr
        self.end_datestr = end_datestr
        self.folder_name = dbcode
        self.ensure_folder_exists()

    def ensure_folder_exists(self):
        os.makedirs(self.folder_name, exist_ok=True)

    def read_code_list(self):
        code_csv_path = os.path.join(self.folder_name, self.download_code_csv)
        try:
            with open(code_csv_path, "r", encoding="utf-8") as f:
                return list(csv.reader(f))
        except FileNotFoundError:
            logger.error(f"File not found: {code_csv_path}")
            raise

    def update_data(self):
        code_list = self.read_code_list()
        filename = ''
        for code, name in code_list:
            start_datestr, end_datestr = self.get_date_range_for_code(name)
            if self.dbtype == "duckdb":
                start_datestr = self.get_duckdb_start_datestr(code)
            elif self.dbtype == "csv":
                filename = self.get_csv_filename(code)
                start_datestr = self.get_csv_start_datestr(filename)
            if start_datestr <= end_datestr:
                self.download_and_store_data(code, start_datestr, end_datestr, filename)
                time.sleep(random.uniform(1, 3))
        if self.dbtype == "duckdb":
            self.remove_duplicates()

    def get_date_range_for_code(self, name):
        start_datestr, end_datestr = self.start_datestr, self.end_datestr
        match = re.search(PATTERN, name)
        if match:
            start_datestr = match.group(1) + "01" if match.group(1) else start_datestr
            end_datestr = match.group(2) + "12" if match.group(2) and match.group(2) != "至今" else end_datestr
        return start_datestr, end_datestr

    def get_csv_filename(self, code):
        return os.path.join(self.folder_name, f"{code.lower()}.csv")

    def get_csv_start_datestr(self, filename):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                last_month_record = next(csv.reader(f))[-1][0]
                return next_month(last_month_record)
        except (FileNotFoundError, StopIteration):
            return self.start_datestr

    def get_duckdb_start_datestr(self, code):
        if os.path.exists(DUCKDB_FILE):
            with duckdb.connect(DUCKDB_FILE) as conn:
                last_month_record = conn.execute(f"SELECT max(time_str) FROM {self.dbcode}_data WHERE code LIKE '%{code}%'").fetchone()[0]
                return next_month(str(last_month_record)) if last_month_record and int(last_month_record)> int(self.start_datestr) else self.start_datestr
        return self.start_datestr

    def download_and_store_data(self, code, start_datestr, end_datestr, filename=''):
        logger.info(f"Downloading data for {code}, from {start_datestr} to {end_datestr}")
        df = stats(code, self.dbcode, start_datestr, end_datestr)
        if df.empty:
            logger.info(f"No new data for {code}")
            return
        
        store_method = getattr(self, f'store_{self.dbtype}')
        store_method(df, filename, code)

    def store_csv(self, df, filename, code):
        df.to_csv(
            filename,
            index=False,
            header=not os.path.exists(filename),
            mode="a",
            encoding="utf-8",
            float_format="%.2f",
        )
        logger.info(f"Successfully downloaded data for {code}")

    def store_duckdb(self, df, code,filename):
        with duckdb.connect(DUCKDB_FILE) as conn:
            conn.register('my_table', df)
            conn.execute(f'INSERT INTO {self.dbcode}_data SELECT * FROM my_table')
            conn.commit()
            conn.close()
            logger.info(f"Data for {code} stored successfully in the database")

    def remove_duplicates(self):
        with duckdb.connect(DUCKDB_FILE) as conn:
        # Assuming 'time_str', 'name', 'code', and 'value' uniquely identify a row
            conn.execute(f'''
                CREATE OR REPLACE TEMPORARY TABLE {self.dbcode}_data_temp AS
                SELECT DISTINCT time_str, name, code, value
                FROM {self.dbcode}_data;

                DELETE FROM {self.dbcode}_data;

                INSERT INTO {self.dbcode}_data 
                SELECT * FROM {self.dbcode}_data_temp;

                DROP TABLE {self.dbcode}_data_temp;
            ''')
            conn.commit()
            conn.close()
            logger.info(f"Duplicate data removed successfully")

def update(dbcode="hgyd", dbtype="duckdb", download_code_csv="code.csv", start_datestr="202001", end_datestr=LAST_MONTH):
    downloader = DataDownloader(
        dbcode=dbcode,
        dbtype=dbtype,
        download_code_csv=download_code_csv,
        start_datestr=start_datestr,
        end_datestr=end_datestr
    )
    downloader.update_data()

# Run the update function if this script is the main program
if __name__ == "__main__":
    update()