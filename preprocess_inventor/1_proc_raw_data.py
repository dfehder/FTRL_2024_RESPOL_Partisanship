import pandas as pd
import os
from dotenv import load_dotenv
from name_proc import *

dotenv_path = "/project/fehder_718/frt2022a/.env"
# load environmental variables
load_dotenv(dotenv_path)

raw_path = os.getenv("RAW_DATA_DIR")
inter_path = os.getenv("INTER_DATA_DIR")



#####################
#  LOAD RAW DATA
#####################

# load raw inventor file from PatentsView
invent_file_path = raw_path + 'inventor.tsv.zip'
inventor = pd.read_csv(invent_file_path, compression='zip', header=0, sep='\t')
inventor = inventor.rename(columns={'id':'inventor_id'})

# load raw patent_inventor file
pi_file_path = raw_path + 'patent_inventor.tsv.zip'
patent_inventor = pd.read_csv(pi_file_path, compression='zip', header=0, sep='\t', dtype={'patent_id':str})

# load location data
loc_file_path = raw_path + 'location.tsv.zip'
location = pd.read_csv(loc_file_path, compression='zip', header=0, sep='\t')
location = location.rename(columns={'id':'location_id'})

# patent data
pat_file_path = raw_path + 'patent.tsv.zip'
patent = pd.read_csv(pat_file_path, compression='zip', 
                     header=0, sep='\t', usecols=['id', 'type', 'date', 'kind'], dtype={'id':str})
patent = patent.rename(columns={'id':'patent_id'})

# read crosswalk data
cross_file_path = inter_path + 'cross_l2county2csa.dta'
crosswalk = pd.read_stata(cross_file_path)
crosswalk = crosswalk.rename(columns={'countycode':'county_fips'})

# # read in and process the cpc patent class data
# # load patent class data
# loc_file_path = raw_path + 'cpc_current.tsv.zip'
# cpc = pd.read_csv(loc_file_path, compression='zip', header=0, sep='\t', dtype={'patent_id':str})

# # now convert from one toplevel patent id column to dummy variables
# cpc = cpc[['patent_id', 'section_id']]
# # cpc['patclass'] = cpc['section_id']
# cpc = cpc.rename(columns={'section_id':'patent_class'})
# # cpc = pd.get_dummies(cpc, columns=['patclass'], drop_first=False)

#####################
#  MERGE RAW DATA
#####################

# add inventor data to patent_inventor
full = patent_inventor.merge(inventor, how='left', on='inventor_id', indicator=True)
print('Output of the inventor X patent_inventor merge:')
print(full['_merge'].value_counts())
print('\n')
full = full.drop(columns=['_merge'])

# add location data
full = full.merge(location, how='left', on='location_id', indicator=True)
print('Output of the full X location merge:')
print(full['_merge'].value_counts())
print('\n')
# Drop 488 observations without location id
full = full[full['_merge']=='both']
full = full.drop(columns=['_merge'])

# merge in patent data
full = full.merge(patent, how='left', on='patent_id', indicator=True)
print('Output of the full X patent merge:')
print(full['_merge'].value_counts())
print('\n')
full = full.drop(columns=['_merge'])

# merge in patent class data
# full = full.merge(cpc, how='left', on='patent_id', indicator=True)
# print('Output of the full X cpc code merge:')
# print(full['_merge'].value_counts())
# print('\n')
# full = full.drop(columns=['_merge'])


########################################
#  RESTRICT TO CORE DATA FOR PAPER (UTILITY PATENTS FROM U.S.)
########################################

# How many rows, inventors, and patents before drops
print('Rows before drops')
print('Total Rows: %s'%(full.shape[0]))
print('\n')
print('Patents before drops')
print(full['patent_id'].nunique())
print('\n')
print('Inventors before drops')
print(full['inventor_id'].nunique())
print('\n')

# Drop any non-us patents
full = full[full['country']=='US']
print('Rows after U.S. drops')
print('Total Rows: %s'%(full.shape[0]))
print('\n')
print('Patents before drops')
print(full['patent_id'].nunique())
print('\n')
print('Inventors before drops')
print(full['inventor_id'].nunique())
print('\n')

# Keep only utility patents
full = full[full['type']=='utility']
print('Rows after utility drops')
print('Total Rows: %s'%(full.shape[0]))
print('\n')
print('Patents before drops')
print(full['patent_id'].nunique())
print('\n')
print('Inventors before drops')
print(full['inventor_id'].nunique())
print('\n')


#####################
#  ADD REGION CODES
#####################

full = full.merge(crosswalk, how='left', on='county_fips', indicator=True)
print('Output of the full X crosswalk merge:')
print(full['_merge'].value_counts())
print('\n')
full = full.drop(columns=['_merge'])

# now import the appended and edited location data
csasub_path = inter_path + 'address_raw_appended_final.csv'
csasub = pd.read_csv(csasub_path)
csasub = csasub.merge(crosswalk, how='left', on='CountyTitle', indicator=True)
print('Output of the csa sub X crosswalk merge:')
print(csasub['_merge'].value_counts())
print('\n')
# keep only the values were we generate a region code
csasub = csasub[csasub['_merge']=='both']
# drop the values that are marked drops
csasub = csasub[csasub['drop']==0]
# keep needed columns
csasub = csasub[['location_id', 'region_code']]
# rename region code so we can merge in
csasub = csasub.rename(columns={'region_code':'alt_region_code'})

# now merge the substitutions back to the original file
full = full.merge(csasub, how='left', on='location_id', indicator=True)

# now take the alternate region code when the original isn't available
full[['region_code', 'alt_region_code']] = full[['region_code', 'alt_region_code']].fillna('')
# take the original region_code if its there, alternate if not
full['reg_code_fin'] = full.apply(lambda x:  x['region_code'] if x['region_code'] else x['alt_region_code'], axis=1)
full[['reg_code_fin']] = full[['reg_code_fin']].fillna('')
full['has_reg_code'] = full.apply(lambda x:  1 if x['reg_code_fin'] else 0, axis=1)


# drop patents from regions in the U.S. that do not have voting rights (so won't have voter reg rolls)
ns = ['PR', 'VI', 'GU']
full['nonstate'] = full.apply(lambda x: 1 if x['state'] in ns else 0, axis=1)
full = full[full['nonstate'] == 0]
print('Rows after non U.S. voter drops')
print('Total Rows: %s'%(full.shape[0]))
print('\n')

# now drop any inventors without a region code
full = full[full['has_reg_code'] == 1]
print('Rows after non region-code drops')
print('Total Rows: %s'%(full.shape[0]))
print('\n')
print('Patents before drops')
print(full['patent_id'].nunique())
print('\n')
print('Inventors before drops')
print(full['inventor_id'].nunique())
print('\n')


# now output the patent_iventor level data
pat_inv_file_path = inter_path + 'patent_inventor_base.csv'
full.to_csv(pat_inv_file_path, index=False)

print('Patent Inventor Level Data Successfully Exported')

###########################
#  MOVE TO INVENTOR LEVEL
############################


# restrict to the required columns
colist = ['patent_id', 'inventor_id', 'location_id', 'date', 'name_first', 'name_last', 
          'male_flag', 'city', 'state', 'reg_code_fin']
full = full[colist]
full = full.rename(columns={'reg_code_fin':'region_code'})

# Extract year 
full['year'] = full['date'].apply(lambda x: int(x[:4]))

# We want to create observations where we say the max year where we observe an inventor 
# in a given commuting region so that we can then look for them in different locales. 
# To do so, we get the last year we observe each inventor in each commuting region.

# this will give us the list of distinct inventors for clean first and last name
name_list = full[['inventor_id', 'name_first', 'name_last']].drop_duplicates()

# now we will create the inventor X commuting region and take the max year
# the list of variables returned
colist = ['inventor_id', 'male_flag', 'region_code','year']
# those we group by
group = ['inventor_id', 'region_code']

inventor = full[colist].groupby(group).agg(max)
inventor = inventor.reset_index()

print('Rows after aggregate')
print('Total Rows: %s'%(inventor.shape[0]))
print('\n')

# now merge back in the names
inventor = inventor.merge(name_list, how='left', on='inventor_id', indicator=True)
print('Output of the inventor X names merge:')
print(inventor['_merge'].value_counts())
print('\n')

# lowercase the first and last names
inventor['name_first'] = inventor['name_first'].str.lower()
inventor['name_last'] = inventor['name_last'].str.lower()

# in case of missing first or last name
inventor = inventor.dropna(subset=['name_first', 'name_last'])
print('Rows after missing name drops')
print('Total Rows: %s'%(inventor.shape[0]))
print('\n')


# split off suffixes
inventor['name_suffix'] = inventor['name_last'].apply(lambda x: suffix(x))

# generate middle initial
inventor['name_middle_initial'] = inventor['name_first'].apply(lambda x: middlei(x))

# now generate clean first and last names
inventor = inventor.rename(columns={'name_first':'name_first_base', 'name_last':'name_last_base'})

inventor['name_first'] = inventor['name_first_base'].apply(lambda x: name_1st(x))
inventor['name_last'] = inventor['name_last_base'].apply(lambda x: name_fam(x))

# code gender
inventor['gender'] = inventor['male_flag'].apply(lambda x: 2 if x >= 1 else 1)

# code time match
inventor['dTime'] = inventor['year'].apply(lambda x: 1 if x > 2016 else 0)



###########################
#  OUTPUT INVENTOR FILE
############################

final_inventor_file_path = inter_path + 'inventor_base.csv'
inventor.to_csv(final_inventor_file_path, index=False)

print('Completed the final output')