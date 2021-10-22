"""
IMPORTING PACKAGES
"""
import wobble
import numpy as np
import matplotlib.pyplot as plt
import glob
import os
from tqdm import tqdm
import tarfile
from astropy.io import fits
import re
import time
from bs4 import BeautifulSoup
from astroquery.eso import Eso as ESO


"""
Functions
"""

def tar_unzipper(data_directory):
    """extracts all tar files in a directory into the same directory"""
    for file in glob.glob(data_directory + '/*.tar'):
        tar = tarfile.open(file, 'r')
        tar.extractall(path = data_directory)
        print('Extracting ' + file + ' to ' + data_directory)

def eso_eprv_preprocessor(data_directory):
    """from HARPS/ESPRESSO CCF data, downloads the necessary calibration files (if missing) and performs pre-processing"""
    #functions go here

#check out os.system