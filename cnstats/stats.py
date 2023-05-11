import time

import pandas as pd
import requests
from retry import retry

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


def easyquery(code, dbcode, *datestrs):
    url = "https://data.stats.gov.cn/easyquery.htm"
    obj = {
        "m": "QueryData",
        "dbcode": dbcode,
        "rowcode": "zb",
        "colcode": "sj",
        "wds": "[]",
        "dfwds": '[{"wdcode":"zb","valuecode":"'
        + code
        + '"},{"wdcode":"sj","valuecode":"'
        + "-".join(datestrs)
        + '"}]',
        "k1": random_timestamp(),
    }
    requests.packages.urllib3.disable_warnings()
    r = requests.post(url, data=obj, headers=header, verify=False)
    return r.json()


@retry(delay=30, tries=20)
def stats(code, dbcode, *datestrs):
    ret = easyquery(code, dbcode, *datestrs)
    if ret["returncode"] == 200:
        data_list = []
        datanodes = ret["returndata"]["datanodes"]
        wdnodes = ret["returndata"]["wdnodes"]
        cname_dict = {item["code"]: item["cname"] for item in wdnodes[0]["nodes"]}
        for i, n in enumerate(datanodes):
            if n["data"]["hasdata"] == True:
                data_list.append(
                    [
                        n["wds"][1]["valuecode"],
                        cname_dict[n["wds"][0]["valuecode"]],
                        n["wds"][0]["valuecode"],
                        n["data"]["data"],
                    ]
                )
        df = pd.DataFrame(data_list, columns=["sj", "name", "zb", "data"]).sort_values(
            ["sj", "zb"]
        )
        return df
    else:
        return pd.DataFrame()
