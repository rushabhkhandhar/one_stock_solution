# -*- coding: utf-8 -*-
"""
Created on Fri Jul 19 19:17:11 2024

@author: Niraj
"""

from screenerScraper import ScreenerScrape
import datetime
sc = ScreenerScrape()

token = sc.getBSEToken("RELIANCE")

a = sc.latestAnnouncements()
d = sc.upcomingResults()

sc.loadScraper(token, consolidated=True)

sd = sc.corporateAnnouncements(datetime.date(2022,7,23), datetime.date(2024,7,20))

data = sc.closePrice()
qaurterly = sc.quarterlyReport(withAddon=True)
pnl = sc.pnlReport(withAddon=True)
bs = sc.balanceSheet(withAddon=True)
cf = sc.cashFLow(withAddon=True)
rat = sc.ratios()
sh = sc.shareHolding(quarterly=False, withAddon=True)
a = sc.annualReports()

# import requests
# Baseurl = "https://api.bseindia.com"
# Headers = {
#                "Access-Control-Allow-Origin" : "*",
#                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
#                "Accept" : "application/json, text/plain, */*",
#                "Accept-Encoding": "gzip, deflate, br, zstd",
#                "Accept-Language" : "en-US,en;q=0.9",
#                "Origin" : "https://www.bseindia.com",
#                "Referer" : "https://www.bseindia.com/",
#            }

# endpoints = {"upcomingAnnoucements" : "/BseIndiaAPI/api/Corpforthresults/w"}

# url = Baseurl + endpoints['upcomingAnnoucements']
# reqSession = requests.Session()
# resp = reqSession.request("GET", url, headers=Headers)
# resp = resp.json()
