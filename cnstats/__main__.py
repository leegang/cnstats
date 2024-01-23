from ast import arg
from .stats import stats
from .zbcode import get_tree
from .download import update
import argparse
import os
import pandas as pd

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="cn-stats", description="获取中国国家统计局网站数据Python包"
    )
    parser.add_argument("-d", "--download", action="store_true", help="下载数据")
    parser.add_argument("-t", "--tree", action="store_const", const="zb", help="列出指标代码")
    parser.add_argument(
        "--dbcode",
        choices=["hgyd", "hgjd", "hgnd"],
        default="hgyd",
        nargs="?",
        help="数据库代码",
    )
    parser.add_argument('-dt',
                        '--dbtype',
                        choices=['csv','duckdb'],
                        default='duckdb',
                        nargs='?',
                        help='数据库类型',
                        )
    parser.add_argument("zbcode", help="指标代码", nargs="?")
    parser.add_argument("date", help="查询日期", nargs="?")
    args = parser.parse_args()

    if args.tree == "zb":
        data_list = get_tree(args.tree, args.dbcode)
        df = pd.DataFrame(data_list, columns=["id", "name"])
        folder_name = args.dbcode
        if not os.path.exists(folder_name):
            os.mkdir(folder_name)

        df.to_csv(f"{folder_name}/code.csv", index=False, encoding="utf-8")


    elif args.download:
        update(args.dbcode,args.dbtype)
    else:
        df = stats(args.zbcode, *args.date)
        if not df.empty:
            print(df.to_csv(index=False, header=False, float_format="%.2f"))
