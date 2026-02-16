# _*_ coding : utf-8 _*_
# @Time : 2024/12/11 15:35
# @Author : Morton
# @File : spider
# @Project : normal-utils

import requests

class Spider:
    count = 0
    fr = None
    gap = 0.1
    header = {
        "user-agent": "",
        "cookie": "",
        "referer": ""
    }

    def __init__(self, header, gap=None):
        self.count = 0
        self.header = header
        if gap is not None:
            self.gap = gap

    def crawl(self, url):
        try:
            response = requests.get(url, headers=self.header)
            res = response.json()
        except Exception as e:
            print(e)
            res = None
        return res
