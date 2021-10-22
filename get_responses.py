
from astroquery.eso import Eso as ESO
from bs4 import BeautifulSoup
import time
import glob
import re
import os

ESO_USERNAME = "andrewjolly"
WAIT_TIME = 60 


def main():

    
    tar_files = ['SAF+HARPS.2007-03-09T20:59:19.434.tar', 'SAF+HARPS.2007-03-13T21:02:07.411.tar']

    data = [("dataset", dataset) for dataset in tar_files]

    print(data)

    eso = ESO()
    eso.login(ESO_USERNAME)

    prepare_response(eso, data)
    confirmation_response = get_confirmation_response(eso, data)
    request_number = get_request_number(confirmation_response)
    
    while not is_eso_ready(eso, request_number):
        print("Eso not ready, sleeping for {} seconds..".format(WAIT_TIME))
        time.sleep(WAIT_TIME)



def prepare_response(eso, data):
    prepare_response = eso._session.request("POST", "http://dataportal.eso.org/rh/confirmation", data=data)

    print("*** start prepare response ***")
    print(prepare_response)
    print("*** end prepare response ***")


def get_confirmation_response(eso, data):
    data += [
        ("requestDescription", ""),
        ("deliveryMediaType", "WEB"),  # OR USB_DISK --> Holy shit what the fuck!
        ("requestCommand", "SELECTIVE_HOTFLY"),
        ("submit", "Submit")
    ]

    confirmation_response = eso._session.request("POST", "http://dataportal.eso.org/rh/requests/{}/submission".format(ESO_USERNAME), data=data)

    print("*** start confirmation response ***")
    print(confirmation_response)
    print("*** end confirmation response ***")

    return confirmation_response


def get_request_number(confirmation_response):
    _ = re.findall("Request #[0-9]+\w", confirmation_response.text)[0].split()[-1]
    request_number = int(_.lstrip("#"))

    print("*** start printing request_number ***")
    print(request_number)
    print("*** end printing request_number ***")


def is_eso_ready(eso, request_number):

    url = "https://dataportal.eso.org/rh/requests/{}".format(ESO_USERNAME)
    check_state = eso._request("GET", url, cache=False)
    root = BeautifulSoup(check_state.text, "html5lib")

    link = root.find(href="/rh/requests/{}/{}".format(ESO_USERNAME, request_number))
    image = link.find_next("img")
    state = image.attrs["alt"]

    print("***** start printing state ****** ")
    print(state)
    print("***** end printing state ****** ")

    if state != "COMPLETE":
        # Remove anything from the astroquery cache.
        for cached_file in glob(os.path.join(eso.cache_location, "*")):
            os.remove(cached_file)
        return False

    return True



if __name__ == '__main__':
    main()
