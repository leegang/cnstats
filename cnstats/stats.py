import time
from os import stat

import pandas as pd
import requests

from .utils import get_periods

header = {
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://data.stats.gov.cn/easyquery.htm?cn=A01",
    "Host": "data.stats.gov.cn",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36 Edg/99.0.1150.36",
    "X-Requested-With": "XMLHttpRequest",
    "Cookie": "_trs_uv=l0krufmy_6_30qm; JSESSIONID=JkGLaObMfWG3_P3_bNKa59cUydvE_nJDUpJOsskem4S-E-wgJeA7!-2135294552; u=1",
}


def random_timestamp():
    return str(int(round(time.time() * 1000)))


def easyquery(code, datestr):
    url = "https://data.stats.gov.cn/easyquery.htm"
    obj = {
        "m": "QueryData",
        "dbcode": "hgyd",
        "rowcode": "zb",
        "colcode": "sj",
        "wds": "[]",
        "dfwds": '[{"wdcode":"zb","valuecode":"'
        + code
        + '"},{"wdcode":"sj","valuecode":"'
        + datestr
        + '"}]',
        "k1": random_timestamp(),
    }
    requests.packages.urllib3.disable_warnings()
    r = requests.post(url, data=obj, headers=header, verify=False)
    return r.json()


def stats(code, /, *args, ret="print"):
    """
    stats(code, datestr)
    stats(code, start_datestr, end_datestr)
    """
    if len(args) == 1:
        periods = (args[0],)
    elif len(args) == 2:
        start_datestr = args[0]
        end_datestr = args[1]
        periods = get_periods(start_datestr, end_datestr)
    else:
        raise TypeError(
            "stats() takes 2 positional arguments but {} were given".format(
                len(args) + 1
            )
        )
    data_list = []
    for datestr in periods:
        ret = easyquery(code, datestr)
        if ret["returncode"] == 200:
            datanodes = ret["returndata"]["datanodes"]
            wdnodes = ret["returndata"]["wdnodes"]
            for i, n in enumerate(datanodes):
                if n["data"]["hasdata"] == True:
                    data_list.append(
                        [
                            n["wds"][1]["valuecode"],
                            wdnodes[0]["nodes"][i]["cname"],
                            n["wds"][0]["valuecode"],
                            n["data"]["data"],
                        ]
                    )
    df = pd.DataFrame(data_list, columns=["sj", "zbname", "zb", "data"])

    if ret == "print":
        print(df.to_csv(index=False, header=False))
    elif ret == "json":
        return df.set_index(["sj", "zb"])["data"].unstack().to_json(double_precision=2)
    elif ret == "dataframe":
        return df
