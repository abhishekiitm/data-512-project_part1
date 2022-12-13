import pandas as pd


def clean_mobility(fip=26125):
    mobility_2020 = pd.read_csv('data_raw/2020_US_Region_Mobility_Report.csv')
    mobility_2021 = pd.read_csv('data_raw/2021_US_Region_Mobility_Report.csv')
    mobility_2022 = pd.read_csv('data_raw/2022_US_Region_Mobility_Report.csv')

    df_2020 = mobility_2020[mobility_2020['census_fips_code'] == fip]
    df_2021 = mobility_2021[mobility_2021['census_fips_code'] == fip]
    df_2022 = mobility_2022[mobility_2022['census_fips_code'] == fip]

    df = df_2020.append(df_2021, ignore_index=True)
    df = df.append(df_2022, ignore_index=True)

    df.to_parquet('data_clean/mobility.pq')


def clean_mortality(fip=26125):
    raw_deaths = pd.read_csv('data_raw/RAW_us_deaths.csv')

    fips_null = raw_deaths['FIPS'].isnull()
    raw_deaths = raw_deaths[~fips_null]

    raw_deaths["FIPS"] = raw_deaths["FIPS"].apply(int)
    raw_deaths = raw_deaths[raw_deaths["FIPS"] == fip]

    id_vars = [x for x in raw_deaths.columns if "/" not in x]
    raw_deaths = pd.melt(raw_deaths, id_vars=id_vars, ignore_index=True)

    raw_deaths.to_parquet('data_clean/mortality.pq')


def clean_cases(fip=26125):
    raw_cases = pd.read_csv('data_raw/RAW_us_confirmed_cases.csv')

    fips_null = raw_cases['FIPS'].isnull()
    raw_cases = raw_cases[~fips_null]

    raw_cases["FIPS"] = raw_cases["FIPS"].apply(int)
    raw_cases = raw_cases[raw_cases["FIPS"] == fip]

    id_vars = [x for x in raw_cases.columns if "/" not in x]
    raw_cases = pd.melt(raw_cases, id_vars=id_vars, ignore_index=True)

    raw_cases.to_parquet('data_clean/cases.pq')


if __name__ == "__main__":
    fip = 26125
    clean_mobility(fip)
    clean_mortality(fip)
    clean_cases(fip)
