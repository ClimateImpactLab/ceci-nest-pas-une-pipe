
import os
import numpy as np
import pandas as pd
import xarray as xr

BASELINE_FILE = (
    '/global/scratch/jiacany/nasa_bcsd/pattern/baseline/' +
    '{baseline_model}/{source_variable}/' +
    '{source_variable}_baseline_1986-2005_r1i1p1_' +
    '{baseline_model}_{{season}}.nc')

BCSD_pattern_files = (
    '/global/scratch/{read_acct}/nasa_bcsd/pattern/SMME_surrogate/' +
    '{scenario}/{source_variable}/{model}/' +
    '{source_variable}_BCSD_{model}_{scenario}_r1i1p1_{{season}}_{year}.nc')

season_month_start = {'DJF': 12, 'MAM': 3, 'JJA': 6, 'SON': 9}


def reshape_days_to_datetime(surrogate, year, season):
    return (
                surrogate.assign_coords(
                        time=xr.DataArray(
                            pd.period_range(
                                '{}-{}-1'.format(
                                    year-int(season == 'DJF'),
                                    season_month_start[season]),
                                periods=len(surrogate.day),
                                freq='D'),
                            coords={'day': surrogate.day},
                            dims=('day',)))
                    .swap_dims({'day': 'time'})
                    .drop('day'))


def get_annual_data(year, **kwargs):
    seasonal_data = []

    for i, season in enumerate(['DJF', 'MAM', 'JJA', 'SON', 'DJF']):
        fp = (BCSD_pattern_files
                    .format(year=year+(i//4), **kwargs)
                    .format(season=season))

        if not os.path.isfile(fp):
            print(fp)

        with xr.open_dataset(fp) as ds:

            ds.load()
            seasonal_data.append(reshape_days_to_datetime(ds, year+(i//4), season))



    ds = xr.concat(seasonal_data, 'time')

    return ds.sel(time=ds['time.year'] == year)
