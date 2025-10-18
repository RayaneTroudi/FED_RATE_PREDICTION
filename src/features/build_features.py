import pandas as pd
# --- 
# BUILD FEATURES ALIGNED ON FOMC MEETING DATES
# --- 
def build_align_all_features_at_correct_meeting_date():
    
    # --- FED FUNDS RATE : TARGET OF OUR MODELS ---
    df_FOMC = pd.read_csv('./data/processed/DFF_PROCESSED.csv')
    df_FOMC.rename(columns={'DATE': 'observation_date'}, inplace=True)
    df_FOMC['observation_date'] = pd.to_datetime(df_FOMC['observation_date'])

    # --- FEATURES  ---
    df_VIX = pd.read_csv('./data/raw/VIXCLS.csv')
    df_UNRATE = pd.read_csv('./data/raw/UNRATE.csv')
    df_SPREAD10Y = pd.read_csv('./data/raw/T10Y2Y.csv')
    df_CPIAUCSL = pd.read_csv('./data/raw/CPIAUCSL.csv')
    
    # --- CONVERT DATE COLUMNS INTO DATETIME TYPE ---
    for df in [df_VIX, df_UNRATE, df_SPREAD10Y, df_CPIAUCSL]:
        date_col = [c for c in df.columns if 'date' in c.lower()][0]
        df.rename(columns={date_col: 'observation_date'}, inplace=True)
        df['observation_date'] = pd.to_datetime(df['observation_date'])

    # --- BUILDING VIX FEATURES ---
    for lag in [1, 7, 14]:
        df_VIX[f'VIX_J_{lag}'] = df_VIX['VIXCLS'].shift(lag)

    df_merged = pd.merge_asof(
        df_FOMC.sort_values('observation_date'),
        df_VIX[['observation_date', 'VIX_J_1', 'VIX_J_7', 'VIX_J_14']].sort_values('observation_date'),
        on='observation_date',
        direction='backward',
        allow_exact_matches=False
    )

    # --- BUILDING UNRATE FEATURES ---
    df_UNRATE['UNRATE_LAG1'] = df_UNRATE['UNRATE'].shift(1)

    df_merged = pd.merge_asof(
        df_merged.sort_values('observation_date'),
        df_UNRATE[['observation_date', 'UNRATE_LAG1']].sort_values('observation_date'),
        on='observation_date',
        direction='backward',
        allow_exact_matches=False
    )

    # --- BUILDING SPREAD FEATURES ---
    df_merged = pd.merge_asof(
        df_merged,
        df_SPREAD10Y[['observation_date', 'T10Y2Y']],
        on='observation_date',
        direction='backward',
        allow_exact_matches=False
    )

    # --- BUILDING CPI FEATURES ---
    df_merged['CPI_LAG1'] = df_CPIAUCSL['CPIAUCSL'].shift(1)
    df_merged = pd.merge_asof(
        df_merged.sort_values('observation_date'),
        df_CPIAUCSL[['observation_date', 'CPIAUCSL']].sort_values('observation_date'),
        on='observation_date',
        direction='backward',
        allow_exact_matches=False
    )

    # --- WRITE THE DATESET IN DEST FILE ---
    df_merged.to_csv('./data/processed/FOMC_ALIGNED_DATA.csv', index=False)


build_align_all_features_at_correct_meeting_date()
