import csv
import logging
import os
import random
import re
import time
from datetime import datetime
import duckdb

from cnstats.stats import stats

from .utils import next_month, previous_month

LAST_MONTH = previous_month(datetime.today().strftime("%Y%m"))

PATTERN = r"\((\d{4})?-(\d{4}|至今)?\)$"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [line:%(lineno)d] - %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


def update(
    dbcode="hgyd",
    dbtype="duckdb",
    download_code_csv="code.csv",
    start_datestr="200001",
    end_datestr=LAST_MONTH,
):
    # 创建数据文件夹
    folder_name = dbcode
    if not os.path.exists(folder_name):
        os.mkdir(folder_name)

    # 按照指定文件名读取指标代码
    code_csv_path = os.path.join(folder_name, download_code_csv)
    if not os.path.exists(code_csv_path):
        raise FileNotFoundError(f"{code_csv_path}不存在")

    # 读取指标代码
    with open(code_csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        code_list = list(reader)

    # 依次下载数据
    for code, name in code_list:
        start_datestr_tmp = start_datestr
        end_datestr_tmp = end_datestr
        # 依照name判断数据时段
        match = re.search(PATTERN, name)
        if match:
            start_datestr_tmp = (
                match.group(1) + "01" if match.group(1) else start_datestr_tmp
            )
            end_datestr_tmp = (
                match.group(2) + "12"
                if match.group(2) and match.group(2) != "至今"
                else end_datestr_tmp
            )

        # 返回 csv 的最大月份
        def get_csv_start_datestr_tmp(filename, code):
            if os.path.exists(filename):
                with open(filename, "r", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    last_month_record = list(reader)[-2][0]
                    start_datestr_tmp = next_month(last_month_record)
            else:
                start_datestr_tmp = start_datestr
            return start_datestr_tmp
        
        # 返回 duckdb 的最大月份
        def get_duckdb_start_datestr_tmp(code):
            if os.path.exists("data/macrodb.duckdb"):
                conn = duckdb.connect("data/macrodb.duckdb")
                cursor = conn.cursor()
                sql_text =  f"SELECT max(distinct(time_str)) FROM {dbcode}_data WHERE code like '%{code}%'"
                logger.info(f"sql_text={sql_text}")
                cursor.execute(sql_text)
                last_month_record = cursor.fetchone()[0]
                conn.close()
                logger.info(f"last_month_record={last_month_record}")
                if last_month_record is None:
                    start_datestr_tmp = start_datestr
                else:
                    last_month_record =str(last_month_record)
                    start_datestr_tmp = next_month(last_month_record)
            else:
                start_datestr_tmp = start_datestr
            return start_datestr_tmp
            
        # 下载数据
        
        if dbtype == "duckdb":
            start_datestr_tmp = get_duckdb_start_datestr_tmp(code)
        elif dbtype == "csv":
            filename = os.path.join(folder_name, f"{code.lower()}.csv")
            start_datestr_tmp = get_csv_start_datestr_tmp(filename, code)
        else:
            raise ValueError(f"dbtype={dbtype}不支持")

        if start_datestr_tmp <= end_datestr_tmp:
            logger.info(f"正在下载{code}数据,{start_datestr_tmp}至{end_datestr_tmp}")
            df = stats(code, dbcode, start_datestr_tmp, end_datestr_tmp)
            if dbtype =='csv':
                if not df.empty:
                    df.to_csv(
                        filename,
                        index=False,
                        header=False,
                        mode="a",
                        encoding="utf-8",
                        float_format="%.2f",
                    )
                    logger.info(f"下载{code}数据成功")
                else:
                    logger.info(f"{code}没有更新数据")
            elif dbtype =='duckdb':
                if not df.empty:
                    conn = duckdb.connect("data/macrodb.duckdb")
                    conn.register('my_table', df)
                    # insert the data to duckdb
                    conn.execute(f'INSERT INTO {dbcode}_data SELECT * FROM my_table')
                    conn.commit()
                    conn.close()
                    logger.info(f"{code}数据入库成功")
                else:
                    logger.info(f"{code}没有更新数据")
    time.sleep(random.randint(1, 3))
