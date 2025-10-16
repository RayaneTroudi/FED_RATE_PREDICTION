

import requests, certifi, re
from bs4 import BeautifulSoup
import pandas as pd

# ===================== SCRAPPED THE RECENT FOMC MEETINGS ===================== #
def scrape_recent_meetings():
    url = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
    html = requests.get(url, verify=certifi.where()).text
    soup = BeautifulSoup(html, "html.parser")

    meetings = []

    for sec in soup.find_all("div", class_="fomc-meeting"):
        month_tag = sec.find("div", class_="fomc-meeting__month")
        date_tag = sec.find("div", class_="fomc-meeting__date")
        if not (month_tag and date_tag):
            continue

        month = month_tag.get_text(strip=True)
        days = date_tag.get_text(strip=True)

        # extrait l'année depuis les liens PDF
        year = None
        for a in sec.find_all("a", href=True):
            m = re.search(r"20\d{2}", a["href"])
            if m:
                year = m.group(0)
                break

        if not year:
            continue

        meetings.append(f"{month} {days}, {year}")

    df_recent = pd.DataFrame(meetings, columns=["Meeting Date"])
    return df_recent


# ===================== SCRAPPED HISTORICAL FOMC MEETINGS ===================== #

def scrape_historical_meetings(start=2000, end=2019):
    base = "https://www.federalreserve.gov/monetarypolicy/fomchistorical{}.htm"
    all_meetings = []

    for y in range(start, end + 1):
        url = base.format(y)
        resp = requests.get(url, verify=certifi.where())
        if resp.status_code != 200:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        for header in soup.find_all(["h5", "strong"]):
            text = header.get_text(strip=True)
            pattern = r"([A-Za-z]+(?:\s*\d{{1,2}}(?:[-/]\d{{1,2}})?)?)\s*Meeting\s*[-–]\s*({})".format(y)
            m = re.match(pattern, text)
            if m:
                all_meetings.append(f"{m.group(1)}, {y}")

    df_hist = pd.DataFrame(all_meetings, columns=["Meeting Date"])
    return df_hist


# ===================== BUILD THE DATASET ===================== #
print("Scrapping FOMC meeting dates ... ")
df_recent = scrape_recent_meetings()
df_hist = scrape_historical_meetings()
df_all = pd.concat([df_hist, df_recent], ignore_index=True)
df_all.drop_duplicates(inplace=True)
df_all.sort_values(by="Meeting Date", inplace=True)

print("Total meetings (historical + recent):", len(df_all))
