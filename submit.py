import json

import requests
import argparse
import urllib.parse
import astropy.time as atime

def submit_post(ra_list , dec_list, jdstart: float, jdend: float, batch: bool, username: str, password: str):
    if len(ra_list) > 1 and not batch:
        print("Error: only one position can be submitted at a time if batch is False.")
        exit(1)
    if batch:
        ra = json.dumps(ra_list)
        dec = json.dumps(dec_list)
        jdstart = json.dumps(jdstart) # start JD for all input target positions.
        jdend = json.dumps(jdend) # end JD for all input target positions.
    else:
        ra = ra_list[0]
        dec = dec_list[0]

    payload = {
        "ra": ra,
        "dec": dec,
        "jdstart": jdstart,
        "jdend": jdend,
        "email": username,
        "userpass": password,
    }
    # fixed IP address/URL where requests are submitted:
    if batch:
        url = "https://ztfweb.ipac.caltech.edu/cgi-bin/batchfp.py/submit"
        r = requests.post(url,auth=("ztffps", "dontgocrazy!"), data=payload)
    else:
        params = urllib.parse.urlencode(payload)
        url = f"https://ztfweb.ipac.caltech.edu/cgi-bin/requestForcedPhotometry.cgi?{params}"
        r = requests.get(url,auth=("ztffps", "dontgocrazy!"))
    
    if r.status_code != 200:
        print("Error: request failed with status code", r.status_code)
        exit(1)
    else:
        print("Request submitted successfully.")
        exit(0)

#--------------------------------------------------
# Main calling program. Ensure "List_of_RA_Dec.txt"
# contains your RA Dec positions.

def submit(ra: list, dec: list, jdstart: float, jdend: float, batch: bool, username: str, password: str):
    if batch:
        i = 0
        ralist, declist = [], []
        for line in lines:
            x = line.split()
            radbl = float(x[0])
            decdbl = float(x[1])

            raval = float("%.7f"%(radbl))
            decval = float("%.7f"%(decdbl))

            ralist.append(raval)
            declist.append(decval)

            i = i + 1
            rem = i % 1500 # Limit submission to 1500 sky positions.

            if rem == 0:
                submit_post(ralist, declist, jdstart, jdend, batch=batch, username=username, password=password)
                ralist = []
                declist = []

        if len(ralist) > 0:
            submit_post(ralist, declist, jdstart, jdend, batch=batch, username=username, password=password)
    else:
        for i in range(len(ra)):
            ralist = [ra[i]]
            declist = [dec[i]]
            submit_post(ralist, declist, jdstart, jdend, batch=batch, username=username, password=password)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--positions",
        help="Path to file containing RA Dec positions.",
        default="List_of_RA_Dec.txt",
    )
    parser.add_argument(
        "--jdstart",
        help="Start JD for all input target positions. Defaults to 365 days ago.",
        default=(atime.Time.now().jd - 365),
    )
    parser.add_argument(
        "--jdend",
        help="End JD for all input target positions. Defaults to now.",
        default=atime.Time.now().jd,
    )
    parser.add_argument(
        "--batch",
        help="Submit all positions in one batch.",
        action="store_true",
    )
    parser.add_argument(
        "--username",
        help="Username for ZTF forced photometry.",
        default=None,
    )
    parser.add_argument(
        "--password",
        help="Password for ZTF forced photometry.",
        default=None,
    )
    args = parser.parse_args()

    if args.username is None:
        print("Error: username must be provided.")
        exit(1)
    if args.password is None:
        print("Error: password must be provided.")
        exit(1)

    lines = []
    try:
        with open(args.positions) as f:
            lines = f.readlines()
        f.close()
    except:
        print("Error: could not open file", args.positions)
        exit(1)
    
    print("Number of (ra,dec) pairs =", len(lines))
    ra, dec = [], []
    for line in lines:
        x = line.split()
        ra.append(float("%.7f"%(float(x[0]))))
        dec.append(float("%.7f"%(float(x[1]))))

    submit(ra, dec, jdstart=args.jdstart, jdend=args.jdend, batch=args.batch, username=args.username, password=args.password)

