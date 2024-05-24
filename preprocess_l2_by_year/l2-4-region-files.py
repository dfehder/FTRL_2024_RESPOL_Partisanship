"""
CREATE DATE: 10/26/2021
CREATED BY: DANIEL FEHDER
DESCRIPTION: Automated creation of regional subset files
"""
import pandas as pd
import os
import sys
from dotenv import load_dotenv

# for now explicitely set the dotenv path. There are ways to make this slicker to not
# require an explicit reference but you need to think about execution environment and architecture

#cluster env path
dotenv_path = "/project/fehder_718/ftrl_2024_respol/.env"
# load environmental variables
load_dotenv(dotenv_path)

sind = str((int(sys.argv[1]) - 1) % 6 + 1)
year = int(2014 + int((int(sys.argv[1]) - 1) / 6))


# now load the base file for that subset
l2inter_path = os.getenv("INTER_DATA_DIR")
l2inter_path = os.path.join(l2inter_path,str(year),'combined')
l2_file = "combine_inter_l2_match_%s.dta"%(sind)
l2_path = os.path.join(l2inter_path, l2_file)

l2_data = pd.read_stata(l2_path)
print('Successfully Loaded L2 Data File: %s'%(l2_file))


region_list = sorted(l2_data['region_code'].unique())

print('number of regions in data: %s'%(len(region_list)))
print("All regions in %s, %s"%(sind, str(year)))
for region in region_list:
    print(str(region))

# now loop through the region list
for region in region_list:
    # get the subset of the data
    subset = l2_data[l2_data['region_code'] == '%s'%(region)]
    # now export the data
    # file name
    export_name = 'l2_region_subset_%s_%s.csv'%(region, sind)
    export_path = os.path.join(l2inter_path, export_name)

    subset.to_csv(export_path, index=False)
    print('Successfully exported file: %s'%(export_name))

print('Finished exporting files')

