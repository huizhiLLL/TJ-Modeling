#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


SEED_ARTICLES = [
    {"article_url": "https://scs.moa.gov.cn/jcyj/201904/t20190425_6410589.htm", "discovery_query": "user_provided_example"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/201912/t20191211_6410763.htm", "discovery_query": "user_provided_example"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202001/t20200109_6410779.htm", "discovery_query": "2020年1月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202001/t20200121_6410789.htm", "discovery_query": "user_provided_example"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202002/t20200211_6410791.htm", "discovery_query": "2020年2月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202006/t20200617_6410889.htm", "discovery_query": "2020年6月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202006/t20200624_6410894.htm", "discovery_query": "2020年6月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202007/t20200708_6410904.htm", "discovery_query": "2020年7月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202007/t20200715_6410909.htm", "discovery_query": "2020年7月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202007/t20200723_6410915.htm", "discovery_query": "2020年7月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202007/t20200729_6410919.htm", "discovery_query": "2020年7月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202008/t20200812_6410927.htm", "discovery_query": "2020年8月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202008/t20200819_6410932.htm", "discovery_query": "2020年8月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202008/t20200826_6410936.htm", "discovery_query": "2020年8月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202009/t20200916_6410942.htm", "discovery_query": "2020年9月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202009/t20200930_6410948.htm", "discovery_query": "2020年9月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202010/t20201014_6410950.htm", "discovery_query": "2020年10月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202010/t20201021_6410952.htm", "discovery_query": "2020年10月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202010/t20201028_6410956.htm", "discovery_query": "2020年10月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202011/t20201117_6410972.htm", "discovery_query": "2020年11月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202011/t20201125_6410975.htm", "discovery_query": "2020年11月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202012/t20201202_6410982.htm", "discovery_query": "2020年11月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202012/t20201229_6411002.htm", "discovery_query": "2020年12月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202101/t20210113_6411009.htm", "discovery_query": "2021年1月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202101/t20210120_6411011.htm", "discovery_query": "user_provided_example"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202101/t20210127_6411021.htm", "discovery_query": "2021年1月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202102/t20210203_6411024.htm", "discovery_query": "2021年1月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202102/t20210210_6411031.htm", "discovery_query": "2021年2月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202102/t20210224_6411036.htm", "discovery_query": "2021年2月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202103/t20210302_6411044.htm", "discovery_query": "2021年2月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202103/t20210311_6411050.htm", "discovery_query": "2021年3月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202103/t20210317_6411058.htm", "discovery_query": "2021年3月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202103/t20210324_6411065.htm", "discovery_query": "2021年3月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202103/t20210331_6411069.htm", "discovery_query": "2021年3月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202104/t20210407_6411075.htm", "discovery_query": "2021年4月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202104/t20210414_6411081.htm", "discovery_query": "2021年4月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202104/t20210421_6411084.htm", "discovery_query": "2021年4月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202104/t20210427_6411088.htm", "discovery_query": "2021年4月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202105/t20210512_6411099.htm", "discovery_query": "2021年5月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202105/t20210518_6411105.htm", "discovery_query": "2021年5月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202105/t20210525_6411112.htm", "discovery_query": "2021年5月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202106/t20210602_6411116.htm", "discovery_query": "2021年5月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202106/t20210609_6411121.htm", "discovery_query": "2021年6月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202106/t20210623_6411130.htm", "discovery_query": "2021年6月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202106/t20210629_6411137.htm", "discovery_query": "2021年6月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202107/t20210706_6411141.htm", "discovery_query": "2021年7月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202107/t20210713_6411145.htm", "discovery_query": "2021年7月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202107/t20210720_6411147.htm", "discovery_query": "2021年7月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202107/t20210728_6411155.htm", "discovery_query": "2021年7月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202108/t20210804_6411157.htm", "discovery_query": "2021年7月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202108/t20210811_6411164.htm", "discovery_query": "2021年8月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202108/t20210817_6411166.htm", "discovery_query": "2021年8月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202108/t20210831_6411180.htm", "discovery_query": "2021年8月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202109/t20210907_6411182.htm", "discovery_query": "2021年9月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202109/t20210914_6411184.htm", "discovery_query": "2021年9月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202109/t20210924_6411187.htm", "discovery_query": "2021年9月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202109/t20210928_6411190.htm", "discovery_query": "2021年9月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202110/t20211013_6411192.htm", "discovery_query": "2021年10月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202110/t20211027_6411199.htm", "discovery_query": "2021年10月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202111/t20211103_6411201.htm", "discovery_query": "2021年10月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202111/t20211109_6411203.htm", "discovery_query": "2021年11月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202112/t20211201_6411212.htm", "discovery_query": "2021年11月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202112/t20211208_6411214.htm", "discovery_query": "2021年12月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202112/t20211215_6411216.htm", "discovery_query": "2021年12月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202112/t20211222_6411221.htm", "discovery_query": "user_provided_example"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202112/t20211229_6411223.htm", "discovery_query": "2021年12月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202201/t20220105_6411225.htm", "discovery_query": "2021年12月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202201/t20220112_6411227.htm", "discovery_query": "user_provided_example"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202201/t20220126_6411233.htm", "discovery_query": "user_provided_example"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202202/t20220210_6411236.htm", "discovery_query": "2022年1月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://www.scs.moa.gov.cn/jcyj/202202/t20220216_6411238.htm", "discovery_query": "2022年2月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202202/t20220222_6411240.htm", "discovery_query": "2022年2月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202203/t20220302_6411244.htm", "discovery_query": "2022年2月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202203/t20220309_6411246.htm", "discovery_query": "2022年3月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202203/t20220316_6411248.htm", "discovery_query": "2022年3月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202203/t20220323_6411251.htm", "discovery_query": "2022年3月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202204/t20220407_6411257.htm", "discovery_query": "2022年3月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202204/t20220413_6411259.htm", "discovery_query": "2022年4月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202204/t20220419_6411261.htm", "discovery_query": "2022年4月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202204/t20220426_6411264.htm", "discovery_query": "2022年4月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202205/t20220509_6411267.htm", "discovery_query": "2022年4月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202205/t20220509_6411269.htm", "discovery_query": "2022年5月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202205/t20220523_6411272.htm", "discovery_query": "2022年5月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202205/t20220530_6411275.htm", "discovery_query": "2022年5月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202206/t20220622_6411285.htm", "discovery_query": "2022年6月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202206/t20220628_6411290.htm", "discovery_query": "2022年6月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202207/t20220707_6411292.htm", "discovery_query": "2022年6月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202207/t20220719_6411297.htm", "discovery_query": "2022年7月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://www.scs.moa.gov.cn/jcyj/202207/t20220726_6411301.htm", "discovery_query": "2022年7月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202208/t20220802_6411303.htm", "discovery_query": "2022年7月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202208/t20220809_6411305.htm", "discovery_query": "2022年8月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://www.scs.moa.gov.cn/jcyj/202208/t20220823_6411310.htm", "discovery_query": "2022年8月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202208/t20220830_6411313.htm", "discovery_query": "2022年8月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202209/t20220907_6411315.htm", "discovery_query": "2022年9月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202209/t20220914_6411317.htm", "discovery_query": "2022年9月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/202209/t20220920_6411319.htm", "discovery_query": "2022年9月份 畜产品和饲料集贸市场价格情况 site:scs.moa.gov.cn/jcyj"},
]


TITLE_PATTERNS = [
    re.compile(r"^\s*(\d{1,2})月份第(\d{1,2})周畜产品和饲料集贸市场价格情况\s*$"),
    re.compile(r"^\s*(\d{1,2})月份第(\d{1,2})?周?畜产品和饲料集贸市场价格情况\s*$"),
    re.compile(r"^\s*(\d{1,2})月(\d{1,2})-(\d{1,2})日畜产品和饲料集贸市场价格情况\s*$"),
]


def init_paths(project_root: Path) -> dict[str, Path]:
    raw_root = project_root / "data" / "raw" / "scs_feed_weekly_articles"
    interim_root = project_root / "data" / "interim" / "scs_feed_weekly_parsed"
    logs_root = project_root / "data" / "logs"
    metadata_root = project_root / "data" / "metadata"
    paths = {
        "raw_root": raw_root,
        "index_root": raw_root / "index_pages",
        "html_root": raw_root / "article_html",
        "text_root": raw_root / "article_text",
        "meta_root": raw_root / "article_meta",
        "interim_root": interim_root,
        "logs_root": logs_root,
        "metadata_root": metadata_root,
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    paths["article_index_csv"] = paths["index_root"] / "article_index.csv"
    paths["wide_csv"] = interim_root / "scs_feed_weekly_prices.csv"
    return paths


def init_logger(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("scs_feed_weekly")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    return logger


def build_session() -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            )
        }
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def normalize_seed_url(url: str) -> str:
    return url.replace("https://www.scs.moa.gov.cn/", "https://scs.moa.gov.cn/")


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def normalize_text(text: str) -> str:
    text = (
        text.replace("\u3000", " ")
        .replace("\xa0", " ")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
    )
    lines = []
    for raw_line in text.split("\n"):
        line = re.sub(r"[ \t]+", " ", raw_line).strip()
        if line:
            lines.append(line)
    return "\n".join(lines)


def rel_path(path: Path, project_root: Path) -> str:
    return path.relative_to(project_root).as_posix()


def extract_week_label(title: str) -> str | None:
    for pattern in TITLE_PATTERNS:
        match = pattern.fullmatch(title)
        if not match:
            continue
        if len(match.groups()) == 2:
            return f"{int(match.group(1))}月第{int(match.group(2))}周"
        if len(match.groups()) == 3:
            return f"{int(match.group(1))}月{int(match.group(2))}-{int(match.group(3))}日"
    return None


def to_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    return round(float(match.group(0)), 6)


def parse_collect_date(compact: str, publish_date: str | None) -> str | None:
    match = re.search(r"采集日为(\d{1,2})月(\d{1,2})日", compact)
    if not match or not publish_date:
        return None
    return f"{publish_date[:4]}-{int(match.group(1)):02d}-{int(match.group(2)):02d}"


def parse_article(article_url: str, html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    title_node = soup.find(["h1", "h2"])
    title = title_node.get_text(" ", strip=True) if title_node else (soup.title.get_text(strip=True) if soup.title else "")
    publish_meta = soup.find("meta", attrs={"name": "publishdate"})
    publish_date = publish_meta.get("content", "")[:10] if publish_meta else None
    compact = re.sub(r"\s+", "", soup.get_text(" ", strip=True))

    corn_match = re.search(r"全国玉米平均价格([0-9.]+)元/公斤", compact)
    soymeal_match = re.search(r"全国豆粕平均价格([0-9.]+)元/公斤", compact)

    return {
        "article_url": article_url,
        "title": title,
        "publish_date": publish_date,
        "week_label": extract_week_label(title),
        "collect_date": parse_collect_date(compact, publish_date),
        "corn_price": to_float(corn_match.group(1)) if corn_match else None,
        "corn_price_unit": "元/公斤" if corn_match else None,
        "soymeal_price": to_float(soymeal_match.group(1)) if soymeal_match else None,
        "soymeal_price_unit": "元/公斤" if soymeal_match else None,
    }


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def dump_json(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def summarize(index_rows: list[dict[str, Any]], wide_rows: list[dict[str, Any]], failed_urls: list[str]) -> dict[str, Any]:
    dates = sorted(row["publish_date"] for row in wide_rows if row.get("publish_date"))
    review_rows = [
        {
            "article_url": row["article_url"],
            "title": row["title"],
            "publish_date": row["publish_date"],
            "validation_flag": row["validation_flag"],
            "validation_notes": row["validation_notes"],
        }
        for row in wide_rows
        if row["validation_flag"] != "ok" or row["validation_notes"]
    ]
    return {
        "seed_article_count": len(index_rows),
        "articles_processed": len(wide_rows),
        "failed_urls": failed_urls,
        "time_coverage": {
            "start": dates[0] if dates else None,
            "end": dates[-1] if dates else None,
        },
        "corn_price_non_null_count": sum(1 for row in wide_rows if row.get("corn_price") is not None),
        "soymeal_price_non_null_count": sum(1 for row in wide_rows if row.get("soymeal_price") is not None),
        "manual_review_rows": review_rows,
    }


def crawl(args: argparse.Namespace) -> dict[str, Any]:
    project_root = Path(__file__).resolve().parents[1]
    paths = init_paths(project_root)
    log_path = paths["logs_root"] / f"scs_feed_weekly_crawl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = init_logger(log_path)
    session = build_session()
    started_at = now_iso()

    index_rows = SEED_ARTICLES[: args.max_articles] if args.max_articles else SEED_ARTICLES[:]
    dedup = {}
    for row in index_rows:
        dedup[normalize_seed_url(row["article_url"])] = {
            "article_url": normalize_seed_url(row["article_url"]),
            "discovery_query": row["discovery_query"],
        }
    index_rows = list(dedup.values())
    write_csv(paths["article_index_csv"], index_rows, ["article_url", "discovery_query"])

    wide_rows: list[dict[str, Any]] = []
    failed_urls: list[str] = []

    for idx, seed in enumerate(index_rows, start=1):
        article_url = seed["article_url"]
        logger.info("处理文章 [%s/%s] %s", idx, len(index_rows), article_url)
        try:
            response = session.get(article_url, timeout=30)
            response.raise_for_status()
            response.encoding = response.apparent_encoding or response.encoding or "utf-8"
            html = response.text
            parsed = parse_article(article_url, html)

            file_stub = f"{parsed['publish_date'] or 'unknown'}_{idx:03d}"
            html_path = paths["html_root"] / f"{file_stub}.html"
            text_path = paths["text_root"] / f"{file_stub}.txt"
            meta_path = paths["meta_root"] / f"{file_stub}.json"
            html_path.write_text(html, encoding="utf-8")
            text_path.write_text(normalize_text(BeautifulSoup(html, "lxml").get_text("\n", strip=True)), encoding="utf-8")

            missing = [field for field in ["corn_price", "soymeal_price"] if parsed.get(field) is None]
            validation_flag = "missing_core_fields" if missing else "ok"
            validation_notes = []
            if missing:
                validation_notes.append(f"missing:{','.join(missing)}")
            if not parsed.get("collect_date"):
                validation_notes.append("collect_date_missing")

            row = {
                **parsed,
                "raw_html_path": rel_path(html_path, project_root),
                "raw_text_path": rel_path(text_path, project_root),
                "parsing_method": "regex_text",
                "validation_flag": validation_flag,
                "validation_notes": "; ".join(validation_notes),
            }
            wide_rows.append(row)

            dump_json(
                meta_path,
                {
                    "article_url": article_url,
                    "title": parsed["title"],
                    "publish_date": parsed["publish_date"],
                    "week_label": parsed["week_label"],
                    "fetched_at": now_iso(),
                    "html_path": rel_path(html_path, project_root),
                    "text_path": rel_path(text_path, project_root),
                    "parse_status": validation_flag,
                    "notes": row["validation_notes"],
                    "discovery_query": seed["discovery_query"],
                },
            )
        except Exception as exc:
            failed_urls.append(article_url)
            logger.exception("文章处理失败：%s error=%s", article_url, exc)
        time.sleep(args.sleep_seconds)

    wide_rows.sort(key=lambda row: (row["publish_date"] or "", row["article_url"]))
    write_csv(
        paths["wide_csv"],
        wide_rows,
        [
            "article_url",
            "title",
            "publish_date",
            "week_label",
            "collect_date",
            "corn_price",
            "corn_price_unit",
            "soymeal_price",
            "soymeal_price_unit",
            "raw_html_path",
            "raw_text_path",
            "parsing_method",
            "validation_flag",
            "validation_notes",
        ],
    )

    summary = summarize(index_rows, wide_rows, failed_urls)
    summary.update(
        {
            "run_started_at": started_at,
            "run_finished_at": now_iso(),
            "article_index_csv": rel_path(paths["article_index_csv"], project_root),
            "wide_csv": rel_path(paths["wide_csv"], project_root),
            "log_path": rel_path(log_path, project_root),
        }
    )
    summary_path = paths["metadata_root"] / "scs_feed_weekly_summary.json"
    review_path = paths["metadata_root"] / "scs_feed_weekly_manual_review.csv"
    dump_json(summary_path, summary)
    write_csv(
        review_path,
        summary["manual_review_rows"],
        ["article_url", "title", "publish_date", "validation_flag", "validation_notes"],
    )
    logger.info("抓取结束：文章=%s 失败=%s", len(wide_rows), len(failed_urls))
    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="抓取 scs 周报中的玉米和豆粕价格")
    parser.add_argument("--max-articles", type=int, default=0, help="仅处理前 N 篇 seed 文章，0 表示全量")
    parser.add_argument("--sleep-seconds", type=float, default=0.02, help="请求间隔秒数")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    summary = crawl(args)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
