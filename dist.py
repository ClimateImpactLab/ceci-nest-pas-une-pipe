import matplotlib
matplotlib.use('Agg')

import xarray as xr
import pandas as pd
import numpy as np
import os, re, glob, shutil
import matplotlib.pyplot as plt
import seaborn
import impactlab_tools.utils.weighting


read_path = (
    '/shares/gcp/outputs/impact_lab_website/global/climate/{agglev}/{rcp_per}/{variable}/' +
    '{variable}_{agglev}_{aggwt}_{model}_{period}.nc')

write_path = (
    '/shares/gcp/outputs/impact_lab_website/global-csvs-v1.0/{agglev}/' +
    'global_{variable}_{rcp}_{period}-{period_end}_{rel}_{variable_descriptor}_percentiles{nat}.csv')

pattern_sources = {
    'rcp45': {
        'pattern1': 'MRI-CGCM3',
        'pattern2': 'GFDL-ESM2G',
        'pattern3': 'MRI-CGCM3',
        'pattern4': 'GFDL-ESM2G',
        'pattern5': 'MRI-CGCM3',
        'pattern6': 'GFDL-ESM2G',
        'pattern27': 'GFDL-CM3',
        'pattern28': 'CanESM2',
        'pattern29': 'GFDL-CM3',
        'pattern30': 'CanESM2', 
        'pattern31': 'GFDL-CM3', 
        'pattern32': 'CanESM2'},
    'rcp85': {
        'pattern1': 'MRI-CGCM3',
        'pattern2': 'GFDL-ESM2G',
        'pattern3': 'MRI-CGCM3',
        'pattern4': 'GFDL-ESM2G',
        'pattern5': 'MRI-CGCM3',
        'pattern6': 'GFDL-ESM2G',
        'pattern28': 'GFDL-CM3',
        'pattern29': 'CanESM2',
        'pattern30': 'GFDL-CM3',
        'pattern31': 'CanESM2', 
        'pattern32': 'GFDL-CM3', 
        'pattern33': 'CanESM2'}}


def get_models(rcp):

    patterns_low = [
        'pattern1', 'pattern2', 'pattern3', 'pattern4', 'pattern5', 'pattern6']

    patterns_high = {
        'rcp45': ['pattern27', 'pattern28', 'pattern29', 'pattern30', 'pattern31', 'pattern32'],
        'rcp85': ['pattern28', 'pattern29', 'pattern30', 'pattern31', 'pattern32', 'pattern33']}

    bcsd = [
        'ACCESS1-0', 'bcc-csm1-1', 'BNU-ESM', 'CanESM2', 'CCSM4', 'CESM1-BGC',
        'CNRM-CM5', 'CSIRO-Mk3-6-0', 'GFDL-CM3', 'GFDL-ESM2G', 'GFDL-ESM2M', 'IPSL-CM5A-LR',
        'IPSL-CM5A-MR', 'MIROC-ESM-CHEM', 'MIROC-ESM', 'MIROC5', 'MPI-ESM-LR', 'MPI-ESM-MR',
        'MRI-CGCM3', 'inmcm4', 'NorESM1-M']

    these_models = patterns_low + bcsd + patterns_high[rcp]
    if rcp == 'rcp45':
        these_models = [m for m in these_models if m != 'GFDL-ESM2G']
    return these_models


def load_model(fp, **kwargs):
    with xr.open_dataset(fp) as ds:
        ds.load()
    
    var = kwargs['variable']
    return xr.Dataset({var: ds[ds.data_vars.keys()[0]]})


def get_data(model, kwargs):
    if model.startswith('pattern'):
        if int(kwargs['period']) > 2006:
            return load_model(read_path.format(model=model, **kwargs))
        else:
            model = pattern_sources[kwargs['rcp']][model]

    loader = load_model
    fp = read_path.format(model=model, **kwargs)

    try:
        return loader(fp, **kwargs)
    except IOError as e:
        print(fp)
        raise

def test_loader():
    ds = get_data(
        'pattern29',
        {'rcp': 'rcp45', 'agglev': 'hierid', 'variable': 'tasmax-over-95F', 'aggwt': 'areawt', 'period': '2020', 'rcp_per': 'rcp45'})

    assert not ds.isnull().any().values()[0]


def sample_data(rcp, outfile):

    rcp_and_period = zip((['historical'] + [rcp]*3), [1986, 2020, 2040, 2080])
    countries = ['USA', 'IND', 'CHN', 'AUS', 'CHL', 'SSD']

    fig = plt.figure()
        
    ax = None

    for i, (rcp_per, period) in enumerate(rcp_and_period):

        kwargs = dict(
            period = str(period),
            variable = 'tasmax-over-95F',
            varname = 'tasmax_gte_95',
            rcp = rcp,
            rcp_per = rcp_per,
            aggwt = 'areawt',
            agglev = 'ISO')

        all_of_them = []
        models = get_models(rcp)
        for model in models:
            all_of_them.append(get_data(model, kwargs))

        ds = xr.concat(
            [xr.Dataset({'tasmax-over-95F': var[var.data_vars.keys()[0]]}) for var in all_of_them],
            dim=pd.Index(models, name='model'))
        
        for j, ISO in enumerate(countries):

            ax = fig.add_subplot(len(countries), 4, j*len(rcp_and_period) + i + 1, sharex=ax, sharey=ax)
            ax.set_title(str(period) + ' ' + ISO)

            ds.sel(ISO=ISO).to_dataframe().plot(ax = ax)
            
    plt.tight_layout()
    fig.savefig(outfile)
    plt.close(fig)


def get_weights(rcp, path='/shares/gcp/climate/BCSD/SMME/SMME-weights/{}_2090_SMME_edited_for_April_2016.tsv'):
    '''
    Gets the weights for models for SMME weighting

    Parameters
    ----------
    path: str
        path to read in the SMME-weights

    Returns
    -------
    pd.Series of model weights 

    '''

    weights = pd.read_csv(path.format(rcp), delimiter='\t')

    weights = weights[['model', 'weight']]

    weights.set_index('model', inplace=True)
    weights.index = weights.index.map(lambda s: s.upper().replace('*','').split('_')[0])

    return weights.weight

def upper_coord_names(da, dim='model'):
    '''
    Coerces coord names to upper case and removes 

    Paramters
    ---------
    ds: Xarray Dataset

    
    Returns
    -------
    Xarray Dataset
    '''

    da.coords[dim] = list(map(lambda x: x.upper(), da.coords[dim].values))

    return da

assert len(get_weights('rcp45')) == 32
assert len(get_weights('rcp85')) == 33


def get_quantiles(da, rcp, quantiles = [0.05, 0.5, 0.95]):
    return impactlab_tools.utils.weighting.weighted_quantile_xr(
            upper_coord_names(da),
            quantiles,
            get_weights(rcp),
            'model',
            values_sorted=False)


def sample_quantiles(rcp, outfile):

    variable = 'tasmax-over-95F'
    varname = 'tasmax_gte_95'

    rcp_and_period = zip((['historical'] + [rcp]*3), [1986, 2020, 2040, 2080])
    countries = ['USA', 'IND', 'CHN', 'AUS', 'CHL', 'SSD']

    fig = plt.figure()
        
    ax = None

    for i, (rcp_per, period) in enumerate(rcp_and_period):

        kwargs = dict(
            period = str(period),
            variable = variable,
            varname = varname,
            aggwt = 'areawt',
            agglev = 'ISO',
            rcp = rcp,
            rcp_per = rcp_per)

        all_of_them = []
        models = get_models(rcp)
        for model in models:
            all_of_them.append(get_data(model, kwargs))

        ds = get_quantiles(xr.concat(
            [var.rename({var.data_vars.keys()[0]: variable}) for var in all_of_them],
            dim=pd.Index(models, name='model'))[variable], rcp)
        
        for j, ISO in enumerate(countries):

            ax = fig.add_subplot(len(countries), 4, j*len(rcp_and_period) + i + 1, sharex=ax, sharey=ax)
            ax.set_title(str(period) + ' ' + ISO)

            ds.sel(ISO=ISO).to_dataframe(name='ISO').plot(ax = ax)
            
    plt.tight_layout()
    fig.savefig(outfile)
    plt.close(fig)


def prep_ds(
            period = '1986',
            variable = 'tasmax-over-95F',
            varname = 'tasmax_gte_95',
            aggwt = 'areawt',
            agglev = 'ISO',
            rcp = 'rcp85'):
    
    rcp_per = rcp if (period != '1986') else 'historical'

    kwargs = dict(
        period = period,
        variable = variable,
        varname = varname,
        aggwt = aggwt,
        agglev = agglev,
        rcp = rcp,
        rcp_per = rcp_per)

    all_of_them = []
    models = get_models(rcp)
    for model in models:
        all_of_them.append(get_data(model, kwargs))

    ds = get_quantiles(xr.concat(
        [var.rename({var.data_vars.keys()[0]: variable}) for var in all_of_them],
        dim=pd.Index(models, name='model'))[variable], rcp)

    return ds, len(all_of_them)


def output_all():

    for agglev in ['ISO', 'hierid']:
        for variable, varname, variable_descriptor in [('tasmax-over-95F', 'tasmax_gte_95', 'days-over-95F')]:
        
            for rcp in ['rcp45', 'rcp85']:

                for rcp_per, period in zip((['historical'] + [rcp]*3), [1986, 2020, 2040, 2080]):

                    ds, nummodels = prep_ds(
                        period=str(period), variable=variable, varname=varname, rcp=rcp, agglev=agglev)

                    print(agglev, variable, rcp, period, nummodels)

                    if rcp_per == 'historical':
                        hist = ds

                    outpath = write_path.format(
                            agglev=agglev,
                            variable=variable,
                            varname=varname,
                            variable_descriptor=variable_descriptor,
                            period=str(period),
                            rcp=rcp,
                            period_end = period + 19,
                            rel = 'absolute',
                            nat='-national' if agglev == 'ISO' else '')

                    if not os.path.isdir(os.path.dirname(outpath)):
                        os.makedirs(os.path.dirname(outpath))
                        
                    if ds.isnull().any():
                        print('problems!!! {}'.format(outpath))
                        continue

                    ds.to_series().unstack('quantile').to_csv(outpath)

                    (ds-hist).to_series().unstack('quantile').to_csv(
                        write_path.format(
                            agglev=agglev,
                            variable=variable,
                            period=str(period),
                            rcp=rcp,
                            period_end = period + 19,
                            rel = 'change-from-hist',
                            varname=varname,
                            variable_descriptor=variable_descriptor,
                            nat='-national' if agglev == 'ISO' else ''))


def main():
    sample_data('rcp45', 'sample_data_plot_rcp45.pdf')
    sample_data('rcp85', 'sample_data_plot_rcp85.pdf')
    sample_quantiles('rcp45', 'sample_quantiles_plot_rcp45.pdf')
    sample_quantiles('rcp85', 'sample_quantiles_plot_rcp85.pdf')

    output_all()


if __name__ == '__main__':
    main()
