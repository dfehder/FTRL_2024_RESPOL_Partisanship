import pandas as pd
import os
import sys

year = int(sys.argv[1])

# set paths

src = '/project/fehder_718/raw_inventors_by_year/inter/by_year_w_imputation/{}.csv'.format(year)

region_path = '/project/fehder_718/raw_inventors_by_year/inter/by_year_w_imputation/{}/'.format(year)

# read inventor data
df = pd.read_csv(src)

# get the list of region codes in the inventor data
regions = df[['region_code']]
# all distinct regions in the inventor data
regions = regions.drop_duplicates().dropna()
# move to list for use in a loop
region_list = regions['region_code'].tolist()
# Final dimensions: 1874 regions

# go through the regions and export seperate csv files
for region in region_list:
    rsub = df[df['region_code'] == region]

    rsub = rsub.sort_values(by=["flag"], ascending=[True], ignore_index = True)
    rsub = rsub.drop_duplicates(subset=["inventor_id"], ignore_index= True)
    outpath = region_path + "researcher_region_%s.csv"%(region)
    print(outpath, rsub.shape)
    rsub.to_csv(outpath, index=False)
    print("finished outputting: %s"%(region))
    print("total inventors in data set: %s"%(rsub.shape[0]))

print('Finished exporting files')

