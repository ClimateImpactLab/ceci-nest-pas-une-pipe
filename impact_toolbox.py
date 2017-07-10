'''
This file is an interface for a set of functions needed in impact forecasting

------------------
My notes and questions 

1. Mortality: Steps below do not represent order
    A. Get covariate values for GDP and Temp
            1. GDP values are some rolling mean from a base line
            2. Temp values are some rolling mean from a base line
            3. In both cases, need baseline year, method of computing mean, and mean period


    B. Get Gamma values
            1. Read in csvv with gamma
            2. Take draw from multivariate distribution to determnine gamma values at given p-value

    C. Define mathematical relationship between covariates and gammas
            1. This is us defnining the dot product or MLE spec
            2. Current implementation uses np.polyval


    D. Determine specific temperature where the minimum value is realized
            1. Evaluate the function, get the minimum   
            2. Get the minimum with np.argmin
            3. Get the min  with impact-common/impactcommon/math/minpoly.findpolymin
            4. I don't understand what he is trying to accomplish with this?

    E. Define the function under the goodmoney assumption
            1. Calculate the response under full adapation
            2. Calculate the response under no income adaption
            3. Take the best response of these two

    F. Evaluate the function under the different 'farmer' scenarios
            1. Income and weather updates
            2. Income but no weather updates
            3. No updates

    H. Something to generate the marginal income effect (climtas_effect). What is this?

    I. Some additional transformation on the curve to constrain the results
            1. Something that compares the regional baseline mins to some 
            other value (specified by climtas effect curve? )

    J. Do something that compares the results from income/weather updates 
    (farm_curvegen) and the climtas_effect (whatever that is)?
'''
import xarray as xr
import numpy as np
import pandas as pd
import metacsv
from six import string_types
import itertools
import toolz
import time
import datafs
import csv
import os


def compute_climate_covariates(path,base_year=None, rolling_window=None):
    ''' 
    Method to calculate climate covariate

    Paramaters
    ----------
    path: str
        glob of paths to climate data

    baseline_year: int
        year from which to generate baseline value

    rolling_window: int
        num years to calculate the rolling covariate average


    .. note: Rolling window only used for full adaptation, otherwise None

    Returns
    -------
    Xarray Dataset
        daily/annual climate values 
    '''


    #compute baseline
    #load datasets up through baseline year
    #compute an average value and set baseline for climate

    #compute annuals
    #for each additional year year that you read in reset baseline value and 
    #update the next years climate with that value
    #output will be a impact_region by time dimension dataset where the value in 
    #for a given time period is the rolling mean temp. 
    #is there a way to do this without looping?

    #baseyear_path = path.format(year=base_year)


    
    ds = xr.open_dataset(path)
    #print(ds)
    
    return ds['tas'] .groupby('hierid').mean()




def compute_gdp_covariates(path, model, scenario, base_year=None, rolling_window=None):
    '''
    Method to calculate climate covariate

    Paramaters
    ----------
    path: str
        glob of paths to GDP data

    baseline_year: int
        year from which to generate baseline value

    rolling_window: int
        num years to calculate the rolling covariate average

    year: int
        baseline year 


    Returns
    -------
    Xarray Dataset
        annual GDP values 

    '''

    #compute baseline
    df = pd.read_csv(path, skiprows=10)
    if not rolling_window:
        df = df.loc[(df['model']==model) & (df['scenario'] == scenario) & (df['year'] == 2015)]
        df['value'] = np.log(df['value'])
        df = df[['hierid', 'value']]
    return df


def gen_gdp_covariates_file(inpath, rolling_window):
    # new_df = pd.DataFrame()
    # df = pd.read_csv(inpath, skiprows=10)
    # df = df.loc[(df['model']=='low') & (df['scenario'] == 'SSP1')]
    # for year in range(2010, 2100):
    #     #get a baseline
    #     # if (year >= 2010) & (year < 2015):
    #     #     #print(year)
    #     #     df = df.loc[(df['year'] >= 2010) & (df['year'] <= 2015)]
    #     #     value = df.groupby('hierid').mean()['value']
    #     #     value= pd.DataFrame(value).reset_index()
    #     #     df1 = pd.DataFrame(value, columns=['hierid', 'value'])
    #     #     df1['year'] = year
    #     #     new_df = new_df.append(df1)
    #     #     #print(new_df.head())

    #     # # # for years in rolling window
    #     # # #For years after 2014 
    #     # if ((year - 2015) >= 0) & ((year - 2015) <= rolling_window):
    #     #     #print(year)
    #     #     df = df.loc[(df['year'] >= 2015) & (df['year'] <= year)]
    #     #     value = df.groupby('hierid').mean()['value']
    #     #     value= pd.DataFrame(value).reset_index()
    #     #     df1 = pd.DataFrame(value, columns=['hierid', 'value'])
    #     #     df1['year'] = year
    #     #     new_df = new_df.append(df1)
    #     #     #print(len(new_df))
    #     #     #print(new_df.head())

    #     if year > 2030:
    #         gap = year-rolling_window
    #         print(year, gap)

    #         df = df.loc[(df['year'] >= gap) & (df['year'] <= year)]
    #         print(df['year'].min(), df['year'].max())
    #         value = df.groupby('hierid').mean()['value']
    #         value= pd.DataFrame(value).reset_index()
    #         df1 = pd.DataFrame(value, columns=['hierid', 'value'])
    #         df1['year'] = year
    #         new_df = new_df.append(df1)
    #             #print(new_df.head())
    #         print(len(new_df))
    pass



def read_csvv(path):
    '''
    Returns the gammas and covariance matrix 

    Returns
    -------
    dict 
    '''

    data = {}

    #constant, climtas, gdp

    with open(path) as file:
        reader = csv.reader(file)
        for row in reader:
            if row[0] == 'gamma':
                data['gamma'] = reader.next()
            if row[0] == 'gammavcv':
                data['gammavcv'] = reader.next()
            if row[0] == 'residvcv':
                data['residvcv'] = reader.next()

    return data['gamma']

def prep_gammas(path):
    '''
    Randomly draws gammas from a multivariate distribution

    Parameters
    ----------
    path: str
        path to file 

    seed: int
        seed for random draw

    Returns
    -------
    dict
    '''
    indices = {'age_cohorts': pd.Index(['infant', 'mid', 'advanced'], name='age')}

    data = [float(num) for num in read_csvv(path)]
    gammas = xr.Dataset()


    for pwr in range(1,5):
            gammas['beta0_pow{}'.format(pwr)] = xr.DataArray(
                data[pwr-1::12], dims=('age',), coords={'age':indices['age_cohorts']})
            gammas['gdp_pow{}'.format(pwr)] = xr.DataArray(
                data[pwr::12], dims=('age',), coords={'age': indices['age_cohorts']})
                gammas['tavg_pow{}'.format(pwr)] = xr.DataArray(
                data[pwr+1::12], dims=('age',), coords={'age': indices['age_cohorts']})

    return gammas
    

def prep_covars(gdp_path, clim_path):

    covars = xr.Dataset()

    gdp = compute_gdp_covariates(gdp_path, 'low', 'SSP1', base_year=2015)
    tas_avg = compute_climate_covariates(clim_path)
    covars['gdp'] = xr.DataArray(gdp['value'], dims=('hierid'), coords={'hierid':gdp['hierid']})
    covars['tavg'] = tas_avg
    return covars



def compute_betas(clim_path, gdp_path, gammas_path, base_year=None,rolling_window=None):
    '''
    Computes the matrices beta*gamma x IR for each covariates 

    1. Calls method to get gammas at given p-value
    2. Calls method toompute gdp covariate
    3. Calls method to compute tas covariate
    4. Computes outer product of 

    Parameters
    ----------


    Returns
    -------

    3 arrays representing 


    '''

    covars = prep_covars(gdp_path, clim_path)
    gammas = prep_gammas(gammas_path)



    betas = xr.Dataset()

    betas['tas'] = (gammas['beta0_pow1'] + gammas['gdp_pow1'] * covars['gdp'] + gammas['tavg_pow1']*covars['tavg'])
    betas['tas-poly-2'] = (gammas['beta0_pow2'] + gammas['gdp_pow2'] * covars['gdp'] + gammas['tavg_pow2']*covars['tavg'])
    betas['tas-poly-3'] = (gammas['beta0_pow3'] + gammas['gdp_pow3'] * covars['gdp'] + gammas['tavg_pow3']*covars['tavg'])
    betas['tas-poly-4'] = (gammas['beta0_pow4'] + gammas['gdp_pow4'] * covars['gdp'] + gammas['tavg_pow4']*covars['tavg'])


    return betas

def get_annual_climate(model_path, year, polynomial):
    '''


    '''

    climate = [xr.open_dataset(model_path.format(polynomial='tas', year=year))]

    for power in range(2,polynomial+1):
        climate.append(xr.open_dataset(model_path.format(year=year,polynomial='tas-poly-{}'.format(power))))


    ds = xr.concat(climate)

    return ds







def compute_annuals(clim_path,
                    gdp_path,
                    gammas_path,
                    model_path, 
                    outpath, 
                    base_year=None, 
                    rolling_window=None):
    '''
    Computes annual impact

    '''

    climate = get_annual_climate(model_path)
   
    betas = compute_betas(clim_path, gdp_path,gammas_path)
    mortality['no_adaptation'] = (
        climate['tas'] * betas['tas'] +
        climate['tas-poly-2'] * betas['tas-poly-2'] +
        climate['tas-poly-3'] * betas['tas-poly-3'])





def pval_thing():
    '''
    Generate a list of pvals
    '''


def goodmoney(ds):
    '''
    Some method to transform data according to goodmoney
    '''


def combine(ds):
    '''
    If we are doing age cohorts, sums the damages for each IR across age cohorts
    '''
    pass


def costs(ds, *args):
    '''
    Some methods to account for costs
    '''



# if __name__ == '__main__':

    # gen_gdp_covariates_file('~/data/gcp_stuff/gdppc-merged.csv', 15)
    # compute_gdp_covariates('~/data/gcp_stuff/gdppc-merged.csv', 'low', 'SSP1', year=2015)
    #read_csvv('/Users/rhodiumgroup/data/gcp_stuff/global_interaction_Tmean-POLY-4-AgeSpec.csvv')
    # compute_climate_covariates('~/data/gcp_stuff/tas_daily_2015.nc4')
    #compute_betas('~/data/gcp_stuff/tas_daily_2015.nc4', '~/data/gcp_stuff/gdppc-merged.csv', '/Users/rhodiumgroup/data/gcp_stuff/global_interaction_Tmean-POLY-4-AgeSpec.csvv', base_year=2015)

