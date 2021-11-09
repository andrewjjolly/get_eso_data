"""
A stripped down version of get_eso to see if I can get away with just downloading the
files via 1 sort of loop. Don't use this script for actual work.
"""

import os
from astropy.extern.configobj.validate import _test
from astropy.io import fits
import numpy as np
import subprocess
import re
import time
import glob
from bs4 import BeautifulSoup
from astroquery.eso import Eso as ESO
import tarfile
import wobble
from tqdm import tqdm
import sys
from astropy.visualization import astropy_mpl_style
from astropy import table
from astropy.coordinates import SkyCoord
from astropy.units import Quantity
from astropy import units as u
from pyvo.dal import tap
import pandas as pd
import requests
import cgi
import etta
from pyvo.utils.http import create_session
import pyvo
import eso_programmatic as esoprog
from pathlib import Path
import shutil

print("\n") #new line because of all the various errors you get from tensorflow importing

ESO_TAP_OBS = "http://archive.eso.org/tap_obs"
ESO_USERNAME = "andrewjolly"
tapobs = tap.TAPService(ESO_TAP_OBS)

DATA_DIR = '/home/z5345592/data/tess_toi/'

def main():

    tess_toi_df = get_tess_toi_df()

    for toi in [2404.01, 576.01, 500.01, 656.01, 431.01, 185.01, 2221.01, 1796.01, 1233.01]:

        toi_name = get_toi_name(tess_toi_df, toi)
        data_dir = make_toi_dir(toi_name)
        toi_coords = get_coords(tess_toi_df, toi)
        # dp_id_list = find_science_files(toi_coords, 'HARPS')
        # download_science_files(dp_id_list, data_dir)
        download_ancillary_files(toi_coords, data_dir)
        extract_files(data_dir)
        move_files(data_dir)
        # ccf_files = get_ccf_files(data_dir)
        # missing_files = identify_missing_files(ccf_files, data_dir)
        # download_files(missing_files, data_dir)
        # move_files(data_dir)
        # extract_files(data_dir)
        # extracted_files_path = extract_files(UNPROCESSED_DIR)
        # # print("preprocessed files: ", preprocessed_files_path)
        # # script_path = '/home/z5345592/projects/get_eso/data/download.sh'  #this line and the next are to manually check that the manually created bash script is working.
        # # subprocess.run([f"sh {script_path}"], shell=True)

def get_tess_toi_df():
    tess_toi_df = etta.download_toi(sort = 'toi')
    print('data fetched!')
    tess_toi_df = tess_toi_df.sort_index(ascending=False) #puts the most recent TOIs first
    tess_toi_df_rows = tess_toi_df.shape[0]
    new_toi = tess_toi_df.first_valid_index()
    new_toi_date = tess_toi_df.iloc[0]['Date Modified']
    new_toi_comments = tess_toi_df.iloc[0]['Comments']
    print("\n")
    print(f'TESS TOI table has {tess_toi_df_rows} TOIs. The most recent is {new_toi} which was updated on {new_toi_date} with the notes: {new_toi_comments}.')

    return(tess_toi_df)

def get_toi_name(tess_toi_df, toi):

    toi_name = tess_toi_df.loc[toi].name #gets the record from the df and slices out the name
    toi_name = toi_name.astype(str) #turn into a string so the replace line following works
    toi_name = toi_name.replace('.','_')

    return(toi_name)

def make_toi_dir(toi_name):
    path = os.path.join(os.path.join(DATA_DIR, toi_name + '/'))
    if not os.path.exists(path):
        os.mkdir(path)
        print('\n')
        print(f'Created data directory for TESS TOI {toi_name}.')
    else:
        print('\n')
        print(f'Data directory for {toi_name} already exists.')

    return path

def get_coords(tess_toi_df, toi):
    """gets the RA & Dec of the target to be able to do a cone search."""
    toi_ra = tess_toi_df.loc[toi].RA
    toi_dec = tess_toi_df.loc[toi].Dec
    toi_coords = SkyCoord(ra = toi_ra, dec = toi_dec, unit = (u.hour, u.degree), frame='icrs')

    return toi_coords

def download_ancillary_files(coords, data_dir):

    sr = 1/60 

    query = """
    SELECT top 1000 access_url 
    FROM ivoa.ObsCore 
    WHERE instrument_name = 'HARPS'
    AND intersects(s_region, circle('', %f, %f, %f))=1
    """ % (coords.ra.degree , coords.dec.degree, sr)
    
    res = tapobs.search(query=query, maxrec=1000)
    print(f'{len(res)} ancillary files identified, commencing download.')
    print('\n')

    session = create_session() #this needed to be added - otherwise not defined in loop below.

    for rec in tqdm(res):
        datalink = pyvo.dal.adhoc.DatalinkResults.from_result_url(rec['access_url'], session=session) #this line needed pyvo not vo
        ancillaries = datalink.bysemantics('#auxiliary')
        for anc in ancillaries:
            # print(anc)
            # for each ancillary, get its access_url, and use it to download the file
            # other useful info available:  print(anc['eso_category'], anc['eso_origfile'], anc['content_length'], anc['access_url'])
            status_code, filepath = esoprog.downloadURL(anc['access_url'], data_dir, session=session)
            if not status_code == 200:
                print(f"File {anc['eso_origfile']} not downloaded")

def count_files(directory):
    count = 0
    for path in Path(directory).rglob('*'):
        if path.is_file():
            count += 1
    return count

def extract_files(directory):
    """extracts all tar files in a directory into the same directory"""
    initial_count = count_files(directory)

    for file in Path(directory).rglob('*.tar'):
        tar = tarfile.open(file, 'r')
        tar.extractall(path = directory)
    
    final_count = count_files(directory)

    print(f'Extracted {final_count - initial_count} files to {directory}.')

    return

def move_files(directory):
    """moves all files in all subdirectories to be in the main directory, deletes the subdirectory"""
    for path in Path(directory).rglob('*'):
        if path.is_file():
            destination = Path(directory) / path.name
            shutil.move(path, destination)

    shutil.rmtree(Path(directory) / 'data')

    return

if __name__ == '__main__':
    main()
    