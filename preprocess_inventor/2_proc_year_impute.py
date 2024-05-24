import pandas as pd
import os
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")

# Load the data
inter_path = '/project/fehder_718/raw_inventors_by_year/inter/'
invent_file = 'inventor_base.csv'
invent_file_path = inter_path + invent_file
dest_path = "/project/fehder_718/raw_inventors_by_year/inter/by_year_w_imputation/{}{}"

df = pd.read_csv(invent_file_path)

# restrict to years from 2014 onwards
df = df[df["year"]>=2014]

# drop unrequired columns
df_proc = df.drop(columns = ["dTime", "_merge"])

# begin breaking into years
years = [ _ for _ in range(2014, 2022)]

df_as_years = {i:None for i in years}

for year in tqdm(years, total = len(years)):
    df_as_years[year] = df_proc[df_proc["year"] == year]

df_grouped_by_inv = df_proc.groupby(["inventor_id"])

inv_to_minmax_years = {}

for tups in tqdm(df_grouped_by_inv, total = len(df_grouped_by_inv)):
    
    inv = tups[0]
    min_year = min(tups[1]["year"])
    max_year = max(tups[1]["year"])
    
    inv_to_minmax_years[inv] = (min_year, max_year)

temp_dfs = {
            2014:None,
            2015:None,
            2016:None,
            2017:None,
            2018:None,
            2019:None,
            2020:None,
            2021:None
           }

# Overall Algo for imputation
# 1. Init in N-1 year
# 2. Find the previous year's authors
# 3. Find the current year's authors
# 4. Find missing authors <- (pya - cya)
# 5. For every missing author:
# 5a. if max year of author < current year, flag = 3
# 5b. if max year of author > current year:
# 5b.i. if uni is same, flag = 1
# 5b.ii. if uni is diff, flag = 2
# 6. add row to current year data set

# Repeat process backwards from 2021 -> 2013
i = 0
for k, v in inv_to_minmax_years.items():
    if v[0]-v[1]>0:
        i+=1

pre_year = 2014
sizes = {}
for curr_year in tqdm(range(2015, 2022), total = 7):
    
    df_prev = df_as_years[pre_year]
    df_prev["flag"] = 1
    
    
     
    df_prev = pd.concat([df_prev, temp_dfs[pre_year]])
    sizes[pre_year] = df_prev.shape[0]
    df_curr = df_as_years[curr_year]
    df_curr["flag"] = 1
    
    set_diff_df = pd.concat([df_prev, df_curr, df_curr]).drop_duplicates(subset=["inventor_id"],keep=False)
    
    
    
    for ind_, row in set_diff_df.iterrows():

        inv_id = row["inventor_id"]
        
        max_year = inv_to_minmax_years[inv_id][1]
        
        if curr_year < max_year:
            set_diff_df.loc[ind_, "flag"] = 2
        else:
            set_diff_df.loc[ind_, "flag"] = 3
    
    temp_dfs[curr_year] = set_diff_df.copy()
    
    pre_year = curr_year

print(sizes)

temp_dfs_backwards = {
            2014:None,
            2015:None,
            2016:None,
            2017:None,
            2018:None,
            2019:None,
            2020:None,
            2021:None
           }

next_year = 2021
sizes = {}
for curr_year in tqdm(range(2021, 2013, -1), total = 7):
    
    df_next = df_as_years[next_year]
    df_next["flag"] = 1
    
    
     
    df_next = pd.concat([df_next, temp_dfs_backwards[next_year]])
    sizes[next_year] = df_next.shape[0]
    
    df_curr = df_as_years[curr_year]
    df_curr["flag"] = 1
    
    set_diff_df = pd.concat([df_next, df_curr, df_curr]).drop_duplicates(subset=["inventor_id"],keep=False)
    
    
    rem_indices = []
    c= 0
    for ind_, row in set_diff_df.iterrows():

        inv_id = row["inventor_id"]
        
        min_year = inv_to_minmax_years[inv_id][0]
        
        if curr_year > min_year:
            set_diff_df.loc[ind_, "flag"] = 2
            c+=1
        
        else:
            rem_indices.append(ind_)
    print(len(rem_indices), len(set_diff_df), c)
    set_diff_df = set_diff_df.drop(index = rem_indices)
    temp_dfs_backwards[curr_year] = set_diff_df.copy()
    
    next_year = curr_year

for curr_year in tqdm(range(2014, 2022), total = 8):
    
    df_final_by_year = pd.concat([df_as_years[curr_year], temp_dfs[curr_year], temp_dfs_backwards[curr_year]])
    
    df_final_by_year.to_csv(dest_path.format(curr_year, ".csv"), index=False)
    
    sizes[curr_year] = df_final_by_year.shape[0]

print(sizes)
print("Finished imputation")