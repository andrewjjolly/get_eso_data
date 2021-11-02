#tl;dr @ 2/11/21 version: the code currently works with the most basic functionality necessary, but I still need to put my password in each time.

#=================================================#
# UPDATED - What is the intention of this script?
#=================================================#

# Current intention for this script is to:

# 1. identify from the list of TESS TOIs any corresponding spectroscopic data available from the ESO instruments on the VLT (ESPRESSO) and La Silla (HARPS)
# 2. download the following files for this data for use with wobble:
#       a) CCF files for each observation
#       b) wavelength files
#       c) other anciallary files
#       d) all science file (AJ question - why not just ALL available files?)
#
# 3. pre-process the files and create a data object for use with wobble (using the wobble functions, from_HARPS and from_ESPRESSO)
# 4. MAYBE - create a plot of the pipeline RVs with associated errors, phase folded on known planets - although it might be easier to roll this into an analysis script.
#
# This script makes extensive use of a script written by Andy Casey / Meg Bedell as well as the ESO data handling tutorials.


#=================================================================#
# Things that are working - things that are left to do / problems - 2 Nov 2021
#=================================================================#

# Working:
# 
# Currently downloads everything correctly (apparently) when given a TOI name. This is just number 2 of the 4 above.       

# Issues that need addressing with current functionality:

# 1. I haven't checked to see whether what is downloaded is EXACTLY the same as if I was to do it manually. I am going to do this when doing
#   first part of analysis.
# 2. I have to put in my ESO password each time despite having passed store_password = True to the eso.login method. This means I can't just set it off.
# 3. File handling is very janky - currently the 'missing files' are downloaded to the cwd and then moved to the correct directory.
# 4. The whole code is a Frankenstein's monster of different methods and I think the solution to number 2 above could at least be solved by
#   using the download ancillary files function below and changing #ancillary to different keywords as per https://www.ivoa.net/rdf/datalink/core/2021-10-28/datalink.html
# 5. The whole code needs refactoring and commenting - maybe to do when further functionality added - see below.
# 6. Printing out from the terminal when the code runs is a mess and doesn't have that much useful info - need to clean up.

# Necessary functionality to be added

# 1. At the moment I need to manually pass in TOI names. I need to add something that does a cone search and then IF it is more than zero - do the things
# 2. There is no ESPRESSO downloading yet
# 3. Be able to run from the command line and check to see if I already have the files (i.e. there could be new TOIs (likely), but also new HARPS/ESPRESSO)


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
import tqdm
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


ESO_TAP_OBS = "http://archive.eso.org/tap_obs"
tapobs = tap.TAPService(ESO_TAP_OBS)

DATA_DIR = '/home/z5345592/data/tess_toi/'
UNPROCESSED_DIR = os.path.join(os.getcwd(), 'unprocessed/') #these three things as they were weren't returning the correct path, just 'unprocessed' as a string.
TEMPLATE_DIR = os.path.join(os.getcwd(), 'template/')
PROCESSED_DIR = os.path.join(os.getcwd(), 'processed/') #check to see if I can make this change to the other two lines - this now seems to work correctly in from_HARPS
ESO_USERNAME = "andrewjolly"


SKIP = 0  # Skip how many batches at the start? (for if you are re-running this..)
BATCH = 2000  # How many datasets should we get per ESO request?
WAIT_TIME = 60  # Seconds between asking ESO if they have prepared our request
# DATA_DIR = os.path.join(os.path.dirname(__file__), "data").


def main():

    tess_toi_df = get_tess_toi_df()

    for toi in [4320.01, 4320.02, 4317.01]:

        toi_name = get_toi_name(tess_toi_df, toi)
        data_dir = make_toi_dir(DATA_DIR, toi_name)
        print(data_dir)
        toi_coords = get_coords(tess_toi_df, toi)
        dp_id_list = find_science_files(toi_coords, 'HARPS')
        download_science_files(dp_id_list, data_dir)
        download_ancillary_files(toi_coords, data_dir)
        extract_files(data_dir)
        ccf_files = get_ccf_files(data_dir)
        missing_files = identify_missing_files(ccf_files, data_dir)
        download_files(missing_files, data_dir)
        move_files(data_dir)
        extract_files(data_dir)
        extracted_files_path = extract_files(UNPROCESSED_DIR)
        # print("preprocessed files: ", preprocessed_files_path)
        # script_path = '/home/z5345592/projects/get_eso/data/download.sh'  #this line and the next are to manually check that the manually created bash script is working.
        # subprocess.run([f"sh {script_path}"], shell=True)

    return


def get_tess_toi_df():
    tess_toi_df = etta.download_toi(sort = 'toi')
    tess_toi_df = tess_toi_df.sort_index(ascending=False) #puts the most recent TOIs first
  
    return(tess_toi_df)

def get_toi_name(tess_toi_df, toi):

    toi_name = tess_toi_df.loc[toi].name #gets the record from the df and slices out the name
    toi_name = toi_name.astype(str) #turn into a string so the replace line following works
    toi_name = toi_name.replace('.','_')

    return(toi_name)

def make_toi_dir(data_directory, toi_name):
    path = os.path.join(os.path.join(DATA_DIR, toi_name + '/'))
    if not os.path.exists(path):
        os.mkdir(path)
    else:
        print('Directory already exists')

    return path

def get_coords(tess_toi_df, toi):
    """gets the RA & Dec of the target to be able to do a cone search."""
    toi_ra = tess_toi_df.loc[toi].RA
    toi_dec = tess_toi_df.loc[toi].Dec
    toi_coords = SkyCoord(ra = toi_ra, dec = toi_dec, unit = (u.hour, u.degree), frame='icrs')
    print(toi_coords)

    return toi_coords

def find_science_files(toi_coords, instrument_name):
    """from a target name and a particular ESO instrument, returns the available science files for downloading."""  

    pos = toi_coords # unsure why this line is necessary but not changing it yet.
   
    sr = 1/60. # search radius of 1 arcmin, always expressed in degrees

    # Cone search: looking for footprints of reduced datasets intersecting a circle of 1' around target
    query = """SELECT *
    FROM ivoa.ObsCore
    WHERE intersects(s_region, circle('', %f, %f, %f))=1
    """ % (pos.ra.degree , pos.dec.degree, sr)

    print(query)

    res = tapobs.search(query=query, maxrec=1000)
    results_table = res.to_table()
    results_table = results_table[results_table['instrument_name'] == instrument_name]
    dp_id_list = list(results_table['dp_id'])
    print("Num matching datasets: %d" % (len(dp_id_list)))
    print(dp_id_list)

    return dp_id_list


def getDispositionFilename(response):
    """Get the filename from the Content-Disposition in the response's http header"""
    contentdisposition = response.headers.get('Content-Disposition')
    if contentdisposition == None:
        return None
    value, params = cgi.parse_header(contentdisposition)
    filename = params["filename"]
    return filename


def writeFile(response, data_dir):
    """Write on disk the retrieved file"""
    if response.status_code == 200:
        # The ESO filename can be found in the response header
        filename = getDispositionFilename( response )
        # Let's write on disk the downloaded FITS spectrum using the ESO filename:
        with open(data_dir + '/' + filename, 'wb') as f:
            f.write(response.content)
        return filename 


def download_science_files(dp_id_list, data_dir):
    """downloads science files from the ESO archive when given a filename"""

    for dp_id in dp_id_list:

        file_url = 'https://dataportal.eso.org/dataportal_new/file/' + dp_id

        response = requests.get(file_url)
        filename = writeFile(response, data_dir)
        if filename:
            print("Saved file: %s" % (filename))
        else:
            print("Could not get file (status: %d)" % (response.status_code))

    return


def download_ancillary_files(coords, data_dir):

    sr = 1/60 

    query = """
    SELECT top 1000 access_url 
    FROM ivoa.ObsCore 
    WHERE instrument_name = 'HARPS'
    AND intersects(s_region, circle('', %f, %f, %f))=1
    """ % (coords.ra.degree , coords.dec.degree, sr)
    
    res = tapobs.search(query=query, maxrec=1000)
    print(res)

    session = create_session() #this needed to be added - otherwise not defined in loop below.

    for rec in (res):
        datalink = pyvo.dal.adhoc.DatalinkResults.from_result_url(rec['access_url'], session=session) #this line needed pyvo not vo
        ancillaries = datalink.bysemantics('#auxiliary')
        for anc in ancillaries:
            print(anc)
            # for each ancillary, get its access_url, and use it to download the file
            # other useful info available:  print(anc['eso_category'], anc['eso_origfile'], anc['content_length'], anc['access_url'])
            status_code, filepath = esoprog.downloadURL(anc['access_url'], data_dir, session=session)
            if status_code == 200:
                print("File {0} downloaded as {1}".format(anc['eso_origfile'], filepath))


def identify_missing_files(ccf_files, data_dir):
    """identifies missing HARPS calibration files and returns them as a list"""
    missing_files = []
    for f in ccf_files:
        sp = fits.open(f)
        header = sp[0].header
        wave_file = header['HIERARCH ESO DRS CAL TH FILE']
        if os.path.isfile(os.path.join(data_dir, wave_file)):
            continue
        else:
            missing_files = np.append(missing_files, wave_file)

    return np.unique(missing_files)


def get_ccf_files(dir):
    """Checks a directory and returns a list of cross-correlation files in the directory"""
    ccf_files = []
    for file in Path(dir).rglob('*ccf*.fits'):
        ccf_files.append(file)

    return ccf_files


def download_files(missing_files, data_dir):
    """downloads the missing calibration files from the ESO"""

    if not os.path.exists(DATA_DIR):
        os.mkdir(DATA_DIR)

    tar_files = get_tar_files(missing_files)

    num_tar_files = len(tar_files)
    num_requests = int(np.ceil(num_tar_files/float(BATCH)))

    remote_paths = []

    print(f"In total we will request {num_tar_files} records in {num_requests} requests")

    for i in range(num_requests):
        if i < SKIP:
            print(f"Skipping {i + 1}")
            continue

        print(f"Starting with batch number {i + 1}/{num_requests}")

        data = [("dataset", tar_file) for tar_file in tar_files[i*BATCH:(i + 1)*BATCH]]

        remote_paths = find_remote_paths(data)

        remote_paths.extend(remote_paths)

        if i < num_requests - 1:
            time.sleep(60)

    # Prepare the script for downloading.
    template_path = os.path.join(TEMPLATE_DIR, "download.sh.template")

    with open(template_path, "r") as template:
        template_contents = template.read()

    script_path = os.path.join(data_dir, "download.sh")

    with open(script_path, "w") as script:
        script.write(template_contents.replace("$$REMOTE_PATHS$$", "\n".join(remote_paths)).replace("$$ESO_USERNAME$$", ESO_USERNAME))

    print(f"Running script at {script_path}...")

    subprocess.run([f"sh {script_path}"], shell=True)

    return


def find_remote_paths(data):

    # log in to eso
    eso = ESO()
    eso.login(ESO_USERNAME, store_password=True)

    prepare_response = eso._session.request("POST", "http://dataportal.eso.org/rh/confirmation", data=data)

    if not prepare_response.ok:
        print("Prepare response failed")
        print("Response content", prepare_response._content)
        exit(1)

    # Additional payload items required for confirmation.
    data += [
        ("requestDescription", ""),
        ("deliveryMediaType", "WEB"),  # OR USB_DISK --> Holy shit what the fuck!
        ("requestCommand", "SELECTIVE_HOTFLY"),
        ("submit", "Submit")
    ]

    confirmation_response = eso._session.request(f"POST", f"http://dataportal.eso.org/rh/requests/{ESO_USERNAME}/submission", data=data)

    if not confirmation_response.ok:
        print("Confirmation response failed")
        print("Response content", confirmation_response._content)
        exit(1)

    # Parse the request number so that we can get a download script from ESO later
    raw_request_number = re.findall("Request #[0-9]+\w", confirmation_response.text)[0].split()[-1]
    request_number = int(raw_request_number.lstrip("#"))

    print("Retrieving remote paths...")

    url = f"https://dataportal.eso.org/rh/requests/{ESO_USERNAME}"

    while not is_eso_ready(eso, url, request_number):
        print(f"Sleeping for {WAIT_TIME} seconds..")
        time.sleep(WAIT_TIME)

    response = eso._request(f"GET", f"{url}/{request_number}/script")

    remote_paths = response.text.split("__EOF__")[-2].split("\n")[1:-2]

    print(f"Found {len(remote_paths)} remote paths for request_number {request_number}")

    # Remove anything from the astroquery cache.
    # for cached_file in glob(os.path.join(eso.cache_location, "*")):
    #     os.remove(cached_file)

    return remote_paths


def is_eso_ready(eso, url, request_number):

    check_state = eso._request("GET", url, cache=False)

    root = BeautifulSoup(check_state.text, "html5lib")

    link = root.find(href=f"/rh/requests/{ESO_USERNAME}/{request_number}")
    image = link.find_next("img")
    state = image.attrs["alt"]

    print(f"Current state {state} on request {request_number}")

    # clean_up
    # for cached_file in glob(os.path.join(eso.cache_location, "*")):
    #     os.remove(cached_file)

    return state == "COMPLETE"


def get_tar_files(missing_files):
    tar_files = []

    for missing_file in missing_files:
        tar_file = f'SAF+{missing_file.split("_")[0]}.tar'
        # Check to see if we have this filename already.
        if not os.path.exists(os.path.join(DATA_DIR, tar_file)):
            tar_files.append(tar_file)

    return tar_files


def extract_files(directory):
    """extracts all tar files in a directory into the same directory"""
    #how can I account for the fact that I might already have tar files in the correct directory.
    for file in glob.glob(directory + '/*.tar'):
        tar = tarfile.open(file, 'r')
        tar.extractall(path = directory)
        print('Extracting ' + file + ' to ' + directory)

    return


def move_files(data_dir):

    for file in Path.cwd().glob('*.tar'):
        Path(file).rename(Path(data_dir + file.name))


if __name__ == '__main__':
    main()