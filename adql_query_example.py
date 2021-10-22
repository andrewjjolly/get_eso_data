import argparse
from pyvo.dal import tap
from astropy import units as u

ESO_TAP_OBS = 'http://archive.eso.org/tap_obs'
tap_obs = tap.TAPService(ESO_TAP_OBS)


def arg_parse():
    """Parse command line arguments."""
    p = argparse.ArgumentParser()
    p.add_argument('ra', help='Target RA in degrees.')
    p.add_argument('dec', help='Target DEC in degrees.')
    p.add_argument('radius', help='Box search radius in arcminutes.')
    p.add_argument('n', help='Max number of rows to retrieve.')
    p.add_argument('out', help='Output directory.')
    return p.parse_args()


def do_query(n, ra, dec, radius):
    ra_min = ra - radius
    ra_max = ra + radius
    dec_min = dec - radius
    dec_max = dec + radius
    query = """
    SELECT TOP {} target, ra, dec, prog_id, instrument, telescope, exp_start, exposure, mjd_obs, dp_cat, datalink_url
    from dbo.raw
    where ra between {} and {}
    and dec between {} and {}
    and dp_cat='SCIENCE'
    and (instrument='ESPRESSO' or instrument='HARPS' or instrument='FEROS')
    """.format(n, ra_min, ra_max, dec_min, dec_max)
    res = tap_obs.search(query=query)
    return res.to_table()


if __name__ == '__main__':
    verbose = False
    args = arg_parse()
    ra = float(args.ra) * u.deg
    dec = float(args.dec) * u.deg
    radius = (float(args.radius) * u.arcmin).to(u.deg)
    n = int(args.n)
    out = args.out

    res = do_query(n, ra.value, dec.value, radius.value)
    res.write(f'{out}.dat', format='ascii.ecsv', overwrite=True)