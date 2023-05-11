import csv
import logging
import os
import re
import time
from datetime import datetime

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

        # 检查文件是否存在
        filename = os.path.join(folder_name, f"{code.lower()}.csv")
        if os.path.exists(f"{filename}"):
            with open(filename, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                last_month_record = list(reader)[-2][0]
            start_datestr_tmp = next_month(last_month_record)

        # 下载数据
        if start_datestr_tmp <= end_datestr_tmp:
            logger.info(f"正在下载{code}数据,{start_datestr_tmp}至{end_datestr_tmp}")
            df = stats(code, dbcode, start_datestr_tmp, end_datestr_tmp)
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
                time.sleep(1)
