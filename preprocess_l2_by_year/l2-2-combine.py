"""
CREATE DATE: 7/9/2021
CREATED BY: DANIEL FEHDER
DESCRIPTION: Automated creation of subset files
"""
import pandas as pd
import numpy as np
import os
import sys
from collections import defaultdict
from dotenv import load_dotenv

# for now explicitely set the dotenv path. There are ways to make this slicker to not
# require an explicit reference but you need to think about execution environment and architecture

#cluster env path
dotenv_path = "/project/fehder_718/ftrl_2024_respol/.env"

# load environmental variables
load_dotenv(dotenv_path)

# 1 - 48
# sind = (argv - 1)  % 6 + 1
# year  = int(2014 + int((argv - 1) / 6))
sind = str((int(sys.argv[2]) - 1) % 6 + 1)
year = int(2014 + int((int(sys.argv[2]) - 1) / 6))

# file locations (make sure paths exist before run)
# path of where to get data
floc = os.path.join(os.getenv("INTER_DATA_DIR"), str(year))
# path for where the data goes
dloc = os.path.join(floc, 'combined')
#dsets = ['preferences_raw', 'demographics_raw']

raw_states = os.listdir(floc)
statemap = {'1':['AK', 'AL', 'AR', 'AZ', 'CA', 'CO'], 
            '2':['CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL'], 
            '3':['IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO'], 
            '4':['MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY'], 
            '5':['OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN'], 
            '6':['TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']}


# get the data set reference we are working with
dset = sys.argv[1]

print(dset)
print(sind)
print(year)

# get the filtered set of files to process
allsets = [elem for elem in raw_states if dset in elem]
allsets = [elem for elem in allsets if elem[:2] in statemap[sind]]


if len(allsets) == 0:
    print(f"No files exist for this state - {sind}, year - {str(year)}")
    sys.exit()

print(allsets)

# initialize the datasets through list comprehension
dflist = [pd.read_pickle(os.path.join(floc, elem)) for elem in allsets]


# combine the datasets
df = pd.concat(dflist)

print(len(df))

# now export
file_name = 'combine_inter_%s_%s.csv'%(dset, sind)
file_path = os.path.join(dloc, file_name)
df.to_csv(file_path, index=False)

