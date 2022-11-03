import pandas as pd


def convert_fips_int_to_string(fips_code):
    fips_code = str(fips_code)
    if len(fips_code) == 4: 
        fips_code = '0' + fips_code
    return fips_code


def clean_cases(raw_cases):
    fips_null = raw_cases['FIPS'].isnull()
    print(f"Dropping {len(fips_null)} rows because FIPS code is not present. \nList of dropped areas: {list(raw_cases[fips_null]['Admin2'])}")
    raw_cases = raw_cases[~fips_null]

    raw_cases["FIPS"] = raw_cases["FIPS"].apply(int)
    raw_cases["FIPS"] = raw_cases["FIPS"].apply(convert_fips_int_to_string)

    id_vars = [x for x in raw_cases.columns if "/" not in x]
    raw_cases = pd.melt(raw_cases, id_vars=id_vars, ignore_index=True)

    raw_cases.to_parquet('data_clean/cases.pq')


def clean_deaths(raw_deaths):
    fips_null = raw_deaths['FIPS'].isnull()
    print(f"Dropping {len(fips_null)} rows because FIPS code is not present. \nList of dropped areas: {list(raw_deaths[fips_null]['Admin2'])}")
    raw_deaths = raw_deaths[~fips_null]

    raw_deaths["FIPS"] = raw_deaths["FIPS"].apply(int)
    raw_deaths["FIPS"] = raw_deaths["FIPS"].apply(convert_fips_int_to_string)

    id_vars = [x for x in raw_deaths.columns if "/" not in x]
    raw_deaths = pd.melt(raw_deaths, id_vars=id_vars, ignore_index=True)

    raw_deaths.to_parquet('data_clean/deaths.pq')


def clean_mandates(mask_mandates_df):
    mask_mandates_df['FIPS'] = mask_mandates_df['FIPS_State']*1000 + mask_mandates_df['FIPS_County']
    mask_mandates_df['FIPS'] = mask_mandates_df['FIPS'].apply(convert_fips_int_to_string)
    mask_mandates_df.to_parquet('data_clean/mask_mandates.pq')


def clean_compliance(mask_compliance_df):
    mask_compliance_df['FIPS'] = mask_compliance_df['COUNTYFP'].apply(convert_fips_int_to_string)
    mask_compliance_df.to_parquet('data_clean/mask_compliance.pq')


raw_cases = pd.read_csv('data_raw/RAW_us_confirmed_cases.csv')
raw_deaths = pd.read_csv('data_raw/RAW_us_deaths.csv')
mask_mandates_df = pd.read_csv('data_raw/mask-mandate-by-county.csv')
mask_compliance_df = pd.read_csv('data_raw/mask-use-by-county.csv')

if __name__ == "__main__":
    clean_cases(raw_cases)
    print("raw cases cleaned")

    clean_deaths(raw_deaths)
    print("raw deaths cleaned")
    
    clean_mandates(mask_mandates_df)
    print("mask mandates cleaned")
    
    clean_compliance(mask_compliance_df)
    print("mask compliance cleaned")

