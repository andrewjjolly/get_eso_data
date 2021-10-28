#=================================================#
# UPDATED - What is the intention of this script?
#=================================================#

# Current intention for this script is to:

# 1. identify from the list of TESS TOIs any corresponding spectroscopic data available from the ESO instruments on the VLT (ESPRESSO) and La Silla (HARPS)
# 2. download the following files for this data for use with wobble:
#       a) CCF files for each observation
#       b) wavelength files
#
# 3. pre-process the files and create a data object for use with wobble (using the wobble functions, from_HARPS and from_ESPRESSO)
# 4. MAYBE - create a plot of the pipeline RVs with associated errors, phase folded on known planets - although it might be easier to roll this into an analysis script.
#
# This script makes extensive use of a script written by Andy Casey / Meg Bedell as well as the ESO data handling tutorials.


#=================================================================#
# Things that are working - things that are left to do / problems
#=================================================================#

# Working:
# 
#   Correctly returns a list of the dp IDs from the ESO archive when given a name of a target.
#   Downloads the science files for the dp IDs provided.
#   Correctly identifies the missing calibration files from CCF files in a directory.
#   Downloads the calibration files fro the eso archive when given a list of calibration files.
#   Using from_HARPS correctly preproccesses a CCF file & calibration files to an hdf5 file for use with wobble#
#
# Immediate to fix:
#   
#   line 164 - Ancillary file identification returning an 'IsADirectoryError' - seems to be a filename issue?
        

# To do:

# Identify and download the correct ancillary files - possibly using the same download methods?
# Identify files I need, files I don't need - automatically delete the ones I don't need (eventually work out a way of not downloading them).
# Write the function that, from exofop gets the target names from the TESS TOIs that have HARPS/ESPRESSO data. Firstly for all current TOIs and then all new TOIs.


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
from pyvo.dal import tap
import pandas as pd
import requests
import cgi
import etta


ESO_TAP_OBS = "http://archive.eso.org/tap_obs"
tapobs = tap.TAPService(ESO_TAP_OBS)

DATA_DIR = '/home/z5345592/data/tess_toi'
UNPROCESSED_DIR = os.path.join(os.getcwd(), 'unprocessed/') #these three things as they were weren't returning the correct path, just 'unprocessed' as a string.
TEMPLATE_DIR = os.path.join(os.getcwd(), 'template/')
PROCESSED_DIR = os.path.join(os.getcwd(), 'processed/') #check to see if I can make this change to the other two lines - this now seems to work correctly in from_HARPS
ESO_USERNAME = "andrewjolly"


# SKIP = 0  # Skip how many batches at the start? (for if you are re-running this..)
# BATCH = 2000  # How many datasets should we get per ESO request?
# WAIT_TIME = 60  # Seconds between asking ESO if they have prepared our request
# DATA_DIR = os.path.join(os.path.dirname(__file__), "data").


def main():

    tess_toi_df = get_tess_toi_df()
    # toi_names = get_toi_names(tess_toi_df)
    # make_toi_dir(DATA_DIR, toi_name)
    # target_coords = 
    # dp_id_list = find_science_files('HD48611', 'HARPS')
    # download_science_files(dp_id_list)
    # ancillary_files = identify_ancillary(UNPROCESSED_DIR)
    # download_ancillary(ancillary_files)
    # ccf_files = get_ccf_files(UNPROCESSED_DIR)
    # missing_files = identify_missing_files(ccf_files)
    # download_files(missing_files)
    # extracted_files_path = extract_files(UNPROCESSED_DIR)
    # preprocess_files(ccf_files)
    # print("preprocessed files: ", preprocessed_files_path)
    # script_path = '/home/z5345592/projects/get_eso/data/download.sh'  #this line and the next are to manually check that the manually created bash script is working.
    # subprocess.run([f"sh {script_path}"], shell=True)

    return



def get_tess_toi_df():
    tess_toi_df = etta.download_toi(sort = 'toi')
    tess_toi_df = tess_toi_df.sort_index(ascending=False) #puts the most recent TOIs first
  
    return(tess_toi_df)

def get_toi_names(tess_toi_df):

    tess_toi_names = tess_toi_df.index
    tess_toi_names = tess_toi_names.astype(str) #turn them into a string
    tess_toi_names = tess_toi_names.str.replace('.', '_') #change the full stop in the toi name to an underscore, probably will be better for filenames etc.

    return(tess_toi_names)

def make_toi_dir(data_directory, folder_name):
    path = os.path.join(os.path.join(DATA_DIR, 'test_name'))
    if not os.path.exists(path):
        os.mkdir(path)
    else:
        print('Directory already exists')


def identify_targets():
    """from a list of TESS TOIs, returns the coordinates to do a cone search with"""

    return target_name


def find_science_files(target_name, instrument_name):
    """from a target name and a particular ESO instrument, returns the available science files for downloading."""  

    pos = SkyCoord.from_name(target_name) # Defining the position via SESAME name resolver, and the search radius, pos now contains the coordinates of target.
   
    print("SESAME coordinates for %s: %s (truncated to millidegrees)\n" % (target_name, pos.to_string()))

    sr = 2.5/60. # search radius of 2.5 arcmin, always expressed in degrees

    # Cone search: looking for footprints of reduced datasets intersecting a circle of 2.5' around target
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


def getDispositionFilename( response ):
    """Get the filename from the Content-Disposition in the response's http header"""
    contentdisposition = response.headers.get('Content-Disposition')
    if contentdisposition == None:
        return None
    value, params = cgi.parse_header(contentdisposition)
    filename = params["filename"]
    return filename


def writeFile( response ):
    """Write on disk the retrieved file"""
    if response.status_code == 200:
        # The ESO filename can be found in the response header
        filename = getDispositionFilename( response )
        # Let's write on disk the downloaded FITS spectrum using the ESO filename:
        with open(UNPROCESSED_DIR + filename, 'wb') as f:
            f.write(response.content)
        return filename 


def download_science_files(dp_id_list):
    """downloads science files from the ESO archive when given a filename"""

    for dp_id in dp_id_list:

        file_url = 'https://dataportal.eso.org/dataportal_new/file/' + dp_id

        response = requests.get(file_url)
        filename = writeFile( response )
        if filename:
            print("Saved file: %s" % (filename))
        else:
            print("Could not get file (status: %d)" % (response.status_code))

    return


def identify_ancillary(directory):
    """identifies the ancillary files linked to a science file"""

    ancillary_files = []
    for file in os.listdir(directory):
        science_file = fits.open(directory + file)
        header = science_file[0].header
        ancillary_file = header['ASSON1']
        ancillary_file = ancillary_file.split('_')
        ancillary_file = os.path.join('https://dataportal.eso.org/dataportal_new/file/', ancillary_file[0])
        ancillary_files=np.append(ancillary_files, ancillary_file)

    return ancillary_files


def download_ancillary(ancillary_files):
    """downloads ancillary files from the ESO"""
    template_path = os.path.join(TEMPLATE_DIR, "download.sh.template")

    with open(template_path, "r") as template:
        template_contents = template.read()

    script_path = os.path.join(DATA_DIR, "download.sh")

    with open(script_path, "w") as script:
        script.write(template_contents.replace("$$REMOTE_PATHS$$", "\n".join(ancillary_files)).replace("$$ESO_USERNAME$$", ESO_USERNAME))

    print(f"Running script at {script_path}...")

    subprocess.run([f"sh {script_path}"], shell=True)

    return


def identify_missing_files(ccf_files):
    """identifies missing HARPS calibration files and returns them as a list"""
    missing_files = []
    for f in ccf_files:
        sp = fits.open(f)
        header = sp[0].header
        wave_file = header['HIERARCH ESO DRS CAL TH FILE']
        if os.path.isfile(os.path.join(UNPROCESSED_DIR, wave_file)):
            continue
        else:
            missing_files = np.append(missing_files, wave_file)

    return np.unique(missing_files)


def get_ccf_files(dir):
    """Checks a directory and returns a list of cross-correlation files in the directory"""
    ccf_files = []
    for file in os.listdir(dir):
        if file.endswith("ccf_G2_A.fits"):
            full_path = os.path.join(dir, file)
            ccf_files.append(full_path)

    return ccf_files


def download_files(missing_files):
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

    script_path = os.path.join(DATA_DIR, "download.sh")

    with open(script_path, "w") as script:
        script.write(template_contents.replace("$$REMOTE_PATHS$$", "\n".join(remote_paths)).replace("$$ESO_USERNAME$$", ESO_USERNAME))

    print(f"Running script at {script_path}...")

    subprocess.run([f"sh {script_path}"], shell=True)

    return


def find_remote_paths(data):

    # log in to eso
    eso = ESO()
    eso.login(ESO_USERNAME)

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
    for cached_file in glob(os.path.join(eso.cache_location, "*")):
        os.remove(cached_file)

    return remote_paths


def is_eso_ready(eso, url, request_number):

    check_state = eso._request("GET", url, cache=False)

    root = BeautifulSoup(check_state.text, "html5lib")

    link = root.find(href=f"/rh/requests/{ESO_USERNAME}/{request_number}")
    image = link.find_next("img")
    state = image.attrs["alt"]

    print(f"Current state {state} on request {request_number}")

    # clean_up
    for cached_file in glob(os.path.join(eso.cache_location, "*")):
        os.remove(cached_file)

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


def preprocess_files(ccf_files):
    "pre-processess data from CCF files & calibration files provided by the ESO for use with wobble"
    #this is just from_HARPS currently - eventually need to change to deal with ESPRESSO (others?)
    
    data = wobble.Data() #create a wobble data object so it can be appended with the information from the CCF files

    for filename in ccf_files:

        try:
            sp = wobble.Spectrum()
            sp.from_HARPS(filename, process = True)
            data.append(sp)
        except Exception as e:
            print("File {0} failed; error: {1}".format(filename, e))

    return data.write(PROCESSED_DIR + '/wobble_data.hdf5')



if __name__ == '__main__':
    main()