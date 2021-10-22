from pyvo.utils.http import create_session
import eso_programmatic as eso
from pyvo.dal import tap
import pyvo
import requests
import json
import glob
import tarfile
import os

ESO_TAP_OBS = 'http://archive.eso.org/tap_obs' #this line and next not in alberto email - added by me
tap_obs = tap.TAPService(ESO_TAP_OBS)

query = """SELECT top 3 s_ra from ivoa.ObsCore where obs_collection='HARPS'"""
res = tap_obs.search(query=query)
print(res)

# session = create_session() #this needed to be added - otherwise not defined in loop below.

# for rec in (res):
#     datalink = pyvo.dal.adhoc.DatalinkResults.from_result_url(rec['access_url'], session=session) #this line needed pyvo not vo
#     ancillaries = datalink.bysemantics('#auxiliary')
#     for anc in ancillaries:
#         # for each ancillary, get its access_url, and use it to download the file
#         # other useful info available:  print(anc['eso_category'], anc['eso_origfile'], anc['content_length'], anc['access_url'])
#         status_code, filepath = eso.downloadURL(anc['access_url'], session=session)
#         if status_code == 200:
#             print("File {0} downloaded as {1}".format(anc['eso_origfile'], filepath))


# """now adding an unzipper - not in ESO email"""

# def extract_files(directory):
#     """extracts all tar files in a directory into the same directory"""
#     #how can I account for the fact that I might already have tar files in the correct directory.
#     for file in glob.glob(directory + '/*.tar'):
#         tar = tarfile.open(file, 'r')
#         tar.extractall(path = directory)
#         print('Extracting ' + file + ' to ' + directory)

#     return

# extract_files(os.getcwd())