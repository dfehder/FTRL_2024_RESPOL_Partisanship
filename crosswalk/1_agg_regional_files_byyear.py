import os
import pandas as pd
import glob


mpath = '/project/fehder_718/ftrl_2024_respol_data/matches/'
dpath = '/project/fehder_718/ftrl_2024_respol/agg_yr/'

years = [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021]

for year in years:
    # location of files
    match_dir = mpath + '{}/'.format(year)
    print('Processing Year: {} from path: {}\n'.format(year, match_dir))
    # set regular search 
    search_path = match_dir + 'matches_region_*'
    # get all files and tell us how many
    all_filenames = glob.glob(search_path)
    print("Total files processed:")
    print(len(all_filenames))
    print('\n')
    #combine all files in the list
    all_matches = pd.concat([pd.read_csv(f) for f in all_filenames])
    # now write the raw data file for future processing
    out_file_path = dpath + 'agg_yr/raw_all_matches_{}.csv'.format(year)
    all_matches.to_csv(out_file_path, index=False)
    print('END 1_agg_regional_files.py: Successfully aggregated and outputted data for year: {}\n'.format(year))

print('Successfully completed loop')