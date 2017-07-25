
import os
import numpy as np
import pandas as pd
import xarray as xr


from climate_toolbox import load_baseline

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

years = range(1982, 2100)

PERIODS = (
    [dict(scenario='rcp45', read_acct='mdelgado', year=y) for y in years] +
    [dict(scenario='rcp85', read_acct='jiacany', year=y) for y in years])

rcp_models = {
    'rcp45':
        list(map(lambda x: dict(model=x[0], baseline_model=x[1]), [
            ('pattern1', 'MRI-CGCM3'),
            ('pattern2', 'GFDL-ESM2G'),
            ('pattern3', 'MRI-CGCM3'),
            ('pattern4', 'GFDL-ESM2G'),
            ('pattern5', 'MRI-CGCM3'),
            ('pattern6', 'GFDL-ESM2G'),
            ('pattern27', 'GFDL-CM3'),
            ('pattern28', 'CanESM2'),
            ('pattern29', 'GFDL-CM3'),
            ('pattern30', 'CanESM2'),
            ('pattern31', 'GFDL-CM3'),
            ('pattern32', 'CanESM2')])),

    'rcp85':
        list(map(lambda x: dict(model=x[0], baseline_model=x[1]), [
            ('pattern1', 'MRI-CGCM3'),
            ('pattern2', 'GFDL-ESM2G'),
            ('pattern3', 'MRI-CGCM3'),
            ('pattern4', 'GFDL-ESM2G'),
            ('pattern5', 'MRI-CGCM3'),
            ('pattern6', 'GFDL-ESM2G'),
            ('pattern28', 'GFDL-CM3'),
            ('pattern29', 'CanESM2'),
            ('pattern30', 'GFDL-CM3'),
            ('pattern31', 'CanESM2'),
            ('pattern32', 'GFDL-CM3'),
            ('pattern33', 'CanESM2')]))}

MODELS = []

for spec in PERIODS:
    for model in rcp_models[spec['scenario']]:
        job = {}
        job.update(spec)
        job.update(model)
        MODELS.append(job)

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

    source_variable = kwargs['source_variable']
    baseline_model = [i for i in rcp_models[kwargs['scenario']] if i['model'] == kwargs['model']][0]['baseline_model']
    
    baseline_file = BASELINE_FILE.format(baseline_model=baseline_model, **kwargs)

    seasonal_baselines = {}
    for season in ['DJF', 'MAM', 'JJA', 'SON']:
        basef = baseline_file.format(season=season)
        seasonal_baselines[season] = load_baseline(
            basef,
            source_variable)

    seasonal_data = []

    for i, season in enumerate(['DJF', 'MAM', 'JJA', 'SON', 'DJF']):
        fp = (BCSD_pattern_files
                    .format(year=year+(i//4), **kwargs)
                    .format(season=season))

        if not os.path.isfile(fp):
            print('skipping {}'.format(fp))
            continue

        with xr.open_dataset(fp) as ds:
            ds.load()
            
        patt = reshape_days_to_datetime(ds, year+(i//4), season)            
        seasonal_data.append(patt + seasonal_baselines[season])

    ds = xr.concat(seasonal_data, 'time')

    ds = ds.sel(time=ds['time.year'] == year)


    # this needs fixing!!!!!
    # also - why does this happen?!?
    ds = ds[source_variable].where(ds[source_variable] < 1e10)

    return ds
