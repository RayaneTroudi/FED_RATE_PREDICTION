# ---
# THIS FILE IS USED TO CLEAN THE RAW DATA AND CREATE A CLEAN VERSION OF THE DATASET
# --- 
from scrapped_data import get_FOMC_meeting_dates
import pandas as pd 
import datetime, math
# --- FUNCTION TO BUILD THE FED RATES MONTHLY

def month_str_to_int(mois):
    mapping = {
        "january": 1, "jan": 1,
        "february": 2, "feb": 2,
        "march": 3, "mar": 3,
        "april": 4, "apr": 4,
        "may": 5,
        "june": 6, "jun": 6,
        "july": 7, "jul": 7,
        "august": 8, "aug": 8,
        "september": 9, "sep": 9,
        "october": 10, "oct": 10,
        "november": 11, "nov": 11,
        "december": 12, "dec": 12
    }
    return mapping.get(mois.lower(), float('nan'))

def build_fed_rates_dataset():
    
    df_fomc_meeting = get_FOMC_meeting_dates(start_date=2017, end_date=2019)
    df_fomc_meeting_preprocessed = pd.DataFrame()
    arr_fomc_meeting_date = []

    for meeting_date in df_fomc_meeting["Meeting Date"]:
        try:
            year = meeting_date.split(', ')[1]
            day = meeting_date.split(', ')[0].split(' ')[1].split('-')[0]
            month_str = meeting_date.split(', ')[0].split(' ')[0].split('/')[0]
            month_num = month_str_to_int(month_str)
            arr_fomc_meeting_date.append(f"{year}-{int(month_num):02d}-{int(day):02d}")
        except Exception as e:
            print(f"Erreur sur {meeting_date}: {e}")
            continue

    df_fomc_meeting_preprocessed["Meeting_Date"] = arr_fomc_meeting_date
    print(df_fomc_meeting_preprocessed)
    df_fomc_meeting_preprocessed.to_csv('../../data/processed/FOMC_MEETING.csv', index=False)
    return df_fomc_meeting_preprocessed



build_fed_rates_dataset()
