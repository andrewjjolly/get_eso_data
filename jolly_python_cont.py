# 1. Identify missing calibration files
#     - get_HARPS_calib.py - script by Meg B? defines a function that checks in the header for the name of the calibration file and creates a text file of the missing files.
#     - consider how this might need to change for ESPRESSO
# 2. Download calibration files identified in 1.
#     - use a script by Andy Casey called 'prepare_download' that (I think) logs into the ESO site and gets the download of the files ready, without actually downloading anything yet.
#     - in a terminal I then change directory to the data directory and run 'cd data; sh download.sh'
# 3. Extract all the .tar files
# 4. Run from_HARPS / from_ESPRESSO (what do these functions actually do?)

import os
from astropy.io import fits
import numpy as np

UNPROCESSED_DIR = os.path.join(os.path.dirname(__file__), "unprocessed")
ESO_USERNAME = "andrewjolly"


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

    SKIP = 0  # Skip how many batches at the start? (for if you are re-running this..)
    BATCH = 2000  # How many datasets should we get per ESO request?
    WAIT_TIME = 60  # Seconds between asking ESO if they have prepared our request
    DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

    if not os.path.exists(DATA_DIR):
        os.mkdir(DATA_DIR)

    tar_files = []

    for missing_file in missing_files:
        tar_file = f'SAF+{missing_file.split("_")[0]}.tar'
        # Check to see if we have this filename already.
        if not os.path.exists(os.path.join(DATA_DIR, tar_file)):
            tar_files.append(tar_file)

    num_tar_files = len(tar_files)
    num_requests = int(np.ceil(num_tar_files/float(BATCH)))

    remote_paths = []

    print(f"In total we will request {num_tar_files} records in {num_requests} requests")

    for i in range (num_requests):
        if i < SKIP:
            print(f"Skipping {i + 1}")
            continue

        print(f"Starting with batch number {i + 1}/{num_requests}")

        data = [("dataset", dataset) for dataset in tar_files[i*BATCH:(i + 1)*BATCH]]

        





    return


def rest_of_download_files():

    for i in range(I):





        data = [("dataset", dataset) for dataset in records[i*BATCH:(i + 1)*BATCH]]

        # Login to ESO.
        eso = ESO()
        eso.login(ESO_USERNAME)

        prepare_response = eso._session.request("POST",
                                                "http://dataportal.eso.org/rh/confirmation", data=data)
        assert prepare_response.ok

        # Additional payload items required for confirmation.
        data += [
            ("requestDescription", ""),
            ("deliveryMediaType", "WEB"),  # OR USB_DISK --> Holy shit what the fuck!
            ("requestCommand", "SELECTIVE_HOTFLY"),
            ("submit", "Submit")
        ]

        confirmation_response = eso._session.request("POST",
                                                     "http://dataportal.eso.org/rh/requests/{}/submission".format(ESO_USERNAME),
                                                     data=data)
        assert confirmation_response.ok

        # Parse the request number so that we can get a download script from ESO later
        _ = re.findall("Request #[0-9]+\w", confirmation_response.text)[0].split()[-1]
        request_number = int(_.lstrip("#"))

        print("Retrieving remote paths for request number {}/{}: {}".format(
            i + 1, I, request_number))

        # Check if ESO is ready for us.
        while True:

            url = "https://dataportal.eso.org/rh/requests/{}".format(ESO_USERNAME)
            check_state = eso._request("GET", url, cache=False)
            root = BeautifulSoup(check_state.text, "html5lib")

            link = root.find(href="/rh/requests/{}/{}".format(
                ESO_USERNAME, request_number))

            image = link.find_next("img")
            state = image.attrs["alt"]

            print("Current state {} on request {} ({}/{})".format(
                state, request_number, i + 1, I))

            if state != "COMPLETE":

                # Remove anything from the astroquery cache.
                for cached_file in glob(os.path.join(eso.cache_location, "*")):
                    os.remove(cached_file)

                print("Sleeping for {} seconds..".format(WAIT_TIME))
                time.sleep(WAIT_TIME)

            else:
                break

        response = eso._request("GET", "{}/{}/script".format(url, request_number))

        paths = response.text.split("__EOF__")[-2].split("\n")[1:-2]
        print("Found {} remote paths for request_number {}".format(
            len(paths), request_number))
        remote_paths.extend(paths)

        # Remove anything from the astroquery cache.
        for cached_file in glob(os.path.join(eso.cache_location, "*")):
            os.remove(cached_file)

        # We have all the remote paths for this request. At ESO's advice, let's
        # wait another 60 seconds before starting our new request.
        if I > i + 1:
            time.sleep(60)

    # Prepare the script for downloading.
    template_path = os.path.join(cwd, "download.sh.template")
    with open(template_path, "r") as fp:
        contents = fp.read()

    script_path = os.path.join(DATA_DIR, "download.sh")
    with open(script_path, "w") as fp:
        fp.write(contents.replace("$$REMOTE_PATHS$$", "\n".join(remote_paths))
                 .replace("$$ESO_USERNAME$$", ESO_USERNAME))

    print("Created script {0}".format(script_path))
    print("Now run `cd {}; sh {}` and enter your ESO password when requested."
          .format(DATA_DIR, os.path.basename(script_path)))



def extract_files(files_path):
    return ""


def preprocess(extracted_files_path):
    return ""


if __name__ == '__main__':
    main()
