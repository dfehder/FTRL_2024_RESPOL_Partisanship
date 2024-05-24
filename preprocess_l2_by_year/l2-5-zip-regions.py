"""
CREATE DATE: 10/26/2021
CREATED BY: DANIEL FEHDER
DESCRIPTION: Take regional subset files and combine into one regional file
"""
import pandas as pd
import os
import sys
import glob
from dotenv import load_dotenv
import shutil
from collections import Counter

#cluster env path
dotenv_path = "/project/fehder_718/ftrl_2024_respol/.env"
# load environmental variables
load_dotenv(dotenv_path)

year = int(sys.argv[1])


inter_data_path = os.getenv("INTER_DATA_DIR")
l2inter_path = os.path.join(inter_data_path,str(year),'combined')

#path for the combined regional files
output_path = os.path.join(inter_data_path,str(year),'l2_region_files')


all_files = os.listdir(l2inter_path)
unique_codes = []
for file in all_files:
    name_array = file.split("_")

    if name_array[0] == 'l2' and name_array[2] == 'subset':
        unique_codes.append(name_array[3])

before_count = len(unique_codes)
print(f"Number of subset regions across all states : {before_count} - Before dropping duplicates")


counts = Counter(unique_codes)

# counts = counts.filter(lambda (key, value) : value > 1)
counts = dict(filter(lambda item: item[1] > 1, counts.items()))
print(f"Regions with subsets across different files : {len(counts)}, \n {counts.keys()}")

unique_codes = list(set(unique_codes))
after_count = len(unique_codes)
print(f"After dropping duplicates: {after_count}")
    
region_list = unique_codes

print('number of regions in data: %s'%(len(region_list)))


# now assign the region we are zipping 
for reg_code in region_list:
    print("We are zipping this region: %s"%(reg_code))
    # get the glob search path
    search_path = os.path.join(l2inter_path, 'l2_region_subset_%s_*'%(reg_code))
    print('GLOB serach path: %s'%(search_path))

    all_filenames = glob.glob(search_path)
    print(all_filenames)
    
    # specify out_file_path
    out_file_path = os.path.join(output_path, 'l2_region_%s.csv'%(reg_code))

    if not len(all_filenames) == 1:
        
        #combine all files in the list
        combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])

        combined_csv.to_csv(out_file_path, index=False)

        print('Successfully wrote: %s'%(out_file_path))
    else:
        shutil.copy(os.path.join(l2inter_path, all_filenames[0]), out_file_path)
        
        print('Successfully copied: %s'%(out_file_path))


print("Succcesfully generated all regions")
