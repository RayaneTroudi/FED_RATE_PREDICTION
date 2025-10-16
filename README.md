# FED PREDICTION RATES

# 1. EXTRACT THE DATA 

    - We have all th interest since 1950's with daily frequency but the rates is update at each FOMC meeting. 
       But most of the macro-economique data are updated with a monthly frequence. 
       We must agregate the fed's rates monthly with a strict respect to the date of the meeting to not lost the timeline of the data.
       Hence, we scrapped the data on the FOMC web site to build a dataframe that contains all meeting date. 
       After that , we can agregate the data monthly and align all the macro-economique data monthly too. 
       