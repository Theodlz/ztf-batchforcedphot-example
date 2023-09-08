import time
import pandas as pd
import numpy as np
import requests
import astropy.time
import json
from astropy.io import ascii
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count

email: str = "<username>"
userpass: int = "<password>"


ZTF_PHOTOMETRY_CODES = {
    0: "Successful execution",
    56: "One or more epochs have photometry measurements that may be impacted by bad (including NaN'd) pixels",
    57: "One or more epochs had no reference image catalog source falling with 5 arcsec",
    58: "One or more epochs had a reference image PSF-catalog that does not exist in the archive",
    59: "One or more epochs may have suspect photometric uncertainties due to early creation date of difference image in production",
    60: "One or more epochs had upsampled diff-image PSF dimensions that were not odd integers",
    61: "One or more epochs had diff-image cutouts that were off the image or too close to an edge",
    62: "Requested start JD was before official survey start date [3/17/18] and was reset to 2018-03-17T00:00:00.0 UT",
    63: "No records (epochs) returned by database query",
    64: "Catastrophic error (see log output)",
    65: "Requested end JD is before official survey start date [3/17/18]",
    255: "Database connection or query execution error (see log output)",
}

def get_lightcurve_url(file):
    return f"https://ztfweb.ipac.caltech.edu{file}"
    
def fetch_lightcurve(row):
    if row['exitcode'] in [63, 64, 65, 255]:
        print(f"Warning: no photometry data found for position (RA, Dec) = ({row['ra']}, {row['dec']}).\n")
        print(f"Error message: {ZTF_PHOTOMETRY_CODES[row['exitcode']]}\n")
        return None
    
    r = requests.get(
        get_lightcurve_url(row['lightcurve']),
        auth=('ztffps', 'dontgocrazy!')
    )
    if r.status_code != 200:
        raise Exception("Failed to retrieve light curves from the ZTF Batch Forced Photometry database.\n")
    
    df = ascii.read(
        r.content.decode(), header_start=0, data_start=1, comment='#'
    ).to_pandas()

    df.columns = df.columns.str.replace(',', '')
    desired_columns = {
        'jd',
        'forcediffimflux',
        'forcediffimfluxunc',
        'diffmaglim',
        'zpdiff',
        'filter',
    }
    if not desired_columns.issubset(set(df.columns)):
        raise ValueError('Missing expected column')
    df.rename(
        columns={'diffmaglim': 'limiting_mag'},
        inplace=True,
    )
    df = df.replace({"null": np.nan})
    df['mjd'] = astropy.time.Time(df['jd'], format='jd').mjd
    df['filter'] = df['filter'].str.replace('_', '')
    df['filter'] = df['filter'].str.lower()
    
    df['mag'] = df['zpdiff'] - 2.5 * np.log10(df['forcediffimflux'])
    df['magerr'] = 1.0857 * df['forcediffimfluxunc'] / df['forcediffimflux']

    snr = df['forcediffimflux'] / df['forcediffimfluxunc'] < 3
    print(f"Nb of epochs with SNR < 3: {len(df[snr])}\n")
    print("vs")
    print(f"Nb of epochs with SNR >= 3: {len(df[~snr])}\n")
    df.loc[snr, 'mag'] = None
    df.loc[snr, 'magerr'] = None

    iszero = df['forcediffimfluxunc'] == 0.0
    df.loc[iszero, 'mag'] = None
    df.loc[iszero, 'magerr'] = None

    isnan = np.isnan(df['forcediffimflux'])
    df.loc[isnan, 'mag'] = None
    df.loc[isnan, 'magerr'] = None

    df = df.replace({np.nan: None})

    drop_columns = list(
        set(df.columns.values)
        - {'mjd', 'ra', 'dec', 'mag', 'magerr', 'limiting_mag', 'filter'}
    )

    df.drop(
        columns=drop_columns,
        inplace=True,
    )
    df['magsys'] = 'ab'

    data = df.to_dict(orient='list')
    return data    

def retrieve():
    r = None
    n_retry = 0
    df_result = None
    while n_retry < 60:
        base_url = 'https://ztfweb.ipac.caltech.edu/cgi-bin/'
        settings = {
            'email': email,
            'userpass': userpass,
            'option': 'All recent jobs',
            'action': 'Query Database'
        }
        r = requests.get(
            base_url + 'getBatchForcedPhotometryRequests.cgi',
            auth=('ztffps', 'dontgocrazy!'),
            params=settings
        )
        if r.status_code == 200:
            print("Script executed normally and queried the ZTF Batch Forced Photometry database.\n")
            df_result: pd.DataFrame = pd.read_html(r.text)[0]
            df_result = df_result.replace({np.nan: None})
            with open("List_of_RA_Dec.txt") as f:
                lines = f.readlines()
            f.close()
            ra, dec = [], []
            for line in lines:
                x = line.split()
                ra.append(float("%.7f"%(float(x[0]))))
                dec.append(float("%.7f"%(float(x[1]))))

            # only keep the rows that correspond to the RA, Dec positions used for the query
            df_result = df_result[df_result['ra'].isin(ra) & df_result['dec'].isin(dec)]
            if len(df_result) < len(ra):
                print(f"Lightcurves not available yet for {len(ra) - len(df_result)} positions.\n")
                print("Retrying in 1 minute...\n")
                time.sleep(60)
                n_retry += 1
            else:
                break
        else:
            print(f"Error code {r.status_code} returned from the ZTF Batch Forced Photometry database.\n")
            print(f"Error message: {r.text}\n")
            print("Retrying in 1 minute...\n")
            time.sleep(60)
            n_retry += 1            

    if n_retry == 60:
        print("Failed to retrieve light curves from the ZTF Batch Forced Photometry database.\n")
        raise Exception("Failed to retrieve light curves from the ZTF Batch Forced Photometry database.\n")
    
    results = {}
    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = []
        for _, row in df_result.iterrows():
            futures.append(executor.submit(fetch_lightcurve, row))
        for future, (_, row) in tqdm(zip(futures, df_result.iterrows()), total=len(df_result), desc='Fetching light curves'):
            results[f"{row['ra']},{row['dec']}"] = future.result()

    #DEBUG: save to disk
    with open('results.json', 'w') as fp:
        json.dump(results, fp, indent=4)
    return results




if __name__ == '__main__':
    results = retrieve()
        