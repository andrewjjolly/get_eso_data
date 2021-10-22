#--------------------------------------------------#
# UPDATED - What is the intention of this script?
#--------------------------------------------------#

# Current intention for this script is to:
# 
# 1. identify from the list of TESS TOIs any corresponding spectroscopic data available from the ESO instruments on the VLT (ESPRESSO) and La Silla (HARPS)
# 2. download the following files for this data for use with wobble:
#       a) CCF files for each observation
#       b) wavelength files
#
# 3. pre-process the files and create a data object for use with wobble (using the wobble functions, from_HARPS and from_ESPRESSO)
# 4. MAYBE - create a plot of the pipeline RVs with associated errors, phase folded on known planets - although it might be easier to roll this into an analysis script.
#
# This script makes extensive use of a script written by Andy Casey / Meg Bedell as well as the ESO data handling tutorials.


#======================================================#
# Things that are working - things that are left to do
#======================================================#
#
# 



import os
from astropy.io import fits
import numpy as np
import subprocess
import re
import time
from glob import glob
from bs4 import BeautifulSoup
from astroquery.eso import Eso as ESO

UNPROCESSED_DIR = os.path.join(os.path.dirname(__file__), "unprocessed")
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "template")
ESO_USERNAME = "andrewjolly"

# downloading files
SKIP = 0  # Skip how many batches at the start? (for if you are re-running this..)
BATCH = 2000  # How many datasets should we get per ESO request?
WAIT_TIME = 60  # Seconds between asking ESO if they have prepared our request
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


def main():
    ccf_files = get_ccf_files(UNPROCESSED_DIR)
    missing_files = identify_missing_files(ccf_files)
    download_files(missing_files)
    # extracted_files_path = extract_files(files_path)
    # preprocessed_files_path = preprocess(extracted_files_path)
    # print("preprocessed files: ", preprocessed_files_path)


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
    ccf_files = []
    for file in os.listdir(dir):
        if file.endswith("ccf_G2_A.fits"):
            full_path = os.path.join(dir, file)
            ccf_files.append(full_path)
    return ccf_files


def download_files(missing_files):

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


def extract_files(files_path):
    return ""


def preprocess(extracted_files_path):
    return ""


if __name__ == '__main__':
    main()
