"""
CREATE DATE: 7/9/2021
CREATED BY: DANIEL FEHDER
DESCRIPTION: Automated creation of subset files
"""
import pandas as pd
import numpy as np
import os
import sys
import l2clean
# from zipfile import ZipFile
from zipfile_deflate64 import ZipFile

from l2datamap import DataMap
from dotenv import load_dotenv
import py7zr
import shutil

# for now explicitely set the dotenv path. There are ways to make this slicker to not
# require an explicit reference but you need to think about execution environment and architecture

dotenv_path = "/project/fehder_718/ftrl_2024_respol/.env"

# load environmental variables
load_dotenv(dotenv_path)

datamap = None
temp_folder_name = None

# initialization data
#dsets = ['preferences_raw', 'demographics_raw', 'match_bias']
dsets = ['l2_match']
datadictref = 'DataProcessingDictionary_by_year.xlsx'
# path of raw data files
floc = os.getenv("L2_RAW_DIR")
print("location of raw data: %s"%(floc))
# path to where processed data goes
dloc = os.getenv("INTER_DATA_DIR")
print("location of processed data: %s"%(dloc))

# returns df from tab file from folder path
def find_tab(folder_path):
    global datamap
    global temp_folder_name

    for data_file_name in os.listdir(os.path.join(folder_path,temp_folder_name)):
        if '.tab' in data_file_name or '.tav' in data_file_name:
            data = pd.read_csv(os.path.join(folder_path, temp_folder_name,data_file_name),sep = '\t',usecols=datamap.dlist, dtype=datamap.dtyper,  encoding='latin')
    shutil.rmtree(os.path.join(folder_path, temp_folder_name))
    return data

def main():
    
    global datamap
    global temp_folder_name
    """Run the main script
    """
    print("filename given: %s"%(sys.argv[1]))

    # get the list of files
    states = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']
    
    
    '''
    # The sys argv corresponds to the year-state_id combination
    
    year = 2014 + (argv - 1) / 50
    state_id = argv % 50
    
    '''
    #stindex = int(sys.argv[1]) - 1
    
    year = int(2014 + (int(sys.argv[1]) - 1) / 51)
   
    stindex = (int(sys.argv[1]) - 1) % 51

    state = states[stindex]
    print('Beginning: %s \n\n\n'%(state))

    temp_folder_name = f"temp_{state}_{str(year)}"

    for ds in dsets:
        print('%s \n\n'%(ds))
        
        # Check if pickle exists
        # If yes, end 
        year_dir_path = os.path.join(dloc, str(year))
        file_name = '%s_%s.pkl'%(state,ds)
        file_path = os.path.join(year_dir_path, file_name)
        if os.path.exists(file_path):
            print(f"File already exists! {file_path}")
            sys.exit(0)
        
        # instantiate the class
        datamap = DataMap(datadictref)
        # this initializes the subset data for this particular dataset
        datamap.subset(ds)
        # initializes dtype dictionary
        datamap.set_dtype()
        
        # this puts all zipped file names in current_state_year
        valid_zip_file_extensions = ['zip', '7z']
        print("Folder location: ", floc)
        file_list = os.listdir(floc)
        print("File list: ", file_list)
        print("State: ", state, " Year: ", str(year))
        current_state_year = []
        for file_name in file_list:
            if f"--{state}--" in file_name and str(year) in file_name:
                current_state_year.append(file_name)
        # extract and import all data to df'
        print("Array of current state and year: ", current_state_year)
        data_list = []
        
        for file_name in current_state_year:
            print("Reading file ...", file_name, "\nPATH: ", os.path.join(floc,file_name))
            if ('.7z' in file_name):
                with py7zr.SevenZipFile(os.path.join(floc,file_name), mode='r') as z:
                    z.extract(os.path.join(floc, temp_folder_name))
                    data_list.append(find_tab(floc))
            if ('.zip' in file_name):
                with ZipFile(os.path.join(floc, file_name)) as myzip:
                #VoterMapping--AK--04-18-2014-HEADERS.tab
                    myzip.extractall(os.path.join(floc, temp_folder_name))
                    data_list.append(find_tab(floc))
        
        #merging logic
        
        if len(data_list) == 0:
            print("Empty data list")
            # Fall out of the loop
            continue
            
        df = pd.concat(data_list).drop_duplicates(subset=['LALVOTERID', 'Voters_FIPS']).reset_index()
        print('Successfully put in dataframe\n')
        print('Number of rows in dataset: %s'%(len(df)))
        for var in datamap.dlist:
            print('Cleaning: %s'%(var))
            # start by getting the function that will be used for the transformation
            transfunc = datamap.clean_map[var]

            # if it is nan then ignore these variables and leave them alone
            if transfunc is np.nan:
                pass
            else:
                # variables in this part of the logic need to be changed
                # apply the right cleaning function to the data (can only be done one)
                df[var] = df[var].apply(lambda x: getattr(l2clean,transfunc)(x))

        # rename the vars so the column names don't suck
        df = df.rename(columns=datamap.rename)

        #df.to_sql(ds, conn, if_exists='append', index = False)
        year_dir_path = os.path.join(dloc, str(year))
        file_name = '%s_%s.pkl'%(state,ds)
        
        file_path = os.path.join(year_dir_path, file_name)
        
        df.to_pickle(file_path)
        # df.to_pickle('%s%s_%s_%s.pkl'%(dloc,state, str(year),ds))
        #conn.commit()
            
    print('END LOOP FOR %s \n\n'%(state))
    #conn.close()

if __name__ == "__main__":
    # run the main loop for the process
    main()
