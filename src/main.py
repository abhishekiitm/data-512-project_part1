import pandas as pd
import matplotlib.pyplot as plt

from datetime import datetime, timedelta

from model import SIRSModel


# load the data
# create time periods of masking and low mobility
# call the models and get the daily estimated infections


def get_first_case(daily_cases):
    return daily_cases.ne(0).idxmax()


cases_df = pd.read_parquet('data_clean/cases.pq')
deaths_df = pd.read_parquet('data_clean/deaths.pq')
mask_compliance_df = pd.read_parquet('data_clean/mask_compliance.pq')
mask_mandates_df = pd.read_parquet('data_clean/mask_mandates.pq')

fip = "26125"

county_cases_df = cases_df[cases_df["FIPS"] == fip]
county_cases_df["Date"] = county_cases_df["variable"].astype("datetime64")
# county_cases_df = county_cases_df.set_index("Date")

county_mandates = mask_mandates_df[mask_mandates_df["FIPS"] == fip]
county_mandates["date"] = county_mandates["date"].astype("datetime64")
# county_mandates = county_mandates.set_index("date")

county_cases_df["daily"] = county_cases_df["value"].diff().fillna(county_cases_df["value"])

merged_df = pd.merge(
    county_cases_df, 
    county_mandates[["FIPS", "Face_Masks_Required_in_Public", "date"]],
    how='left',
    left_on=['FIPS', 'Date'],
    right_on=['FIPS', 'date']
)

merged_df = merged_df.fillna({"Face_Masks_Required_in_Public": "No"})
merged_df = merged_df.set_index("Date")
merged_df["mask"] = merged_df["Face_Masks_Required_in_Public"].apply(lambda x: 1 if x=="Yes" else 0)
merged_df = merged_df[merged_df.index <= pd.Timestamp('2021-10-01')]
merged_df['daily_7d_ma'] = merged_df['daily'].rolling(7).mean()

merged_df['infections'] = merged_df['daily_7d_ma'].rolling(10).sum()*9

time_infection_to_recovery = 10
gamma = 1/time_infection_to_recovery

infection_immunity_period = 60
delta = 1/infection_immunity_period

first_case = get_first_case(merged_df['daily'])
first_infection = first_case - timedelta(days=time_infection_to_recovery)
end_date = datetime(year=2021, month=10, day=1)

mask_mandate = merged_df['Face_Masks_Required_in_Public']
mask_mandate = list(mask_mandate[mask_mandate.index >= first_infection])

beta_wo_mask = 0.3
beta_mask = 0.3
mobility_impact = 0.9

population = 1274000

sirs_model = SIRSModel(
    N = population,
    beta_wo_mask = beta_wo_mask,
    beta_mask = beta_mask,
    gamma = gamma,
    delta = delta,
    s0 = population-1,
    i0 = 1,
    r0 = 0,
    first_infection = first_infection,
    end_date = end_date,
    mask_mandate=mask_mandate
)

results = sirs_model.simulate()

res_df = pd.DataFrame(list(zip(results[3], results[0], results[1], results[2])), columns=['date', 's_t', 'i_t', 'r_t'])
res_df = res_df.set_index('date')

res_df.plot()


pass

