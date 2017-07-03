'''
This file is an interface for a set of functions needed in impact forecasting



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
import datafs
import csv


def compute_climate_covariates(path,base_year,rolling_window):
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



    #return ds



def compute_gdp_covariates(path, base_year,rolling_window):
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

    Returns
    -------
    Xarray Dataset
        annual GDP values 

    '''

    #compute baseline


    #compute rolling mean after baseline value


def compute_gdp_baseline(path):
    '''

    '''
    #if value is zero what do you actually do
    df = pd.read_csv(path)




def read_csvv(path):
    '''
    Returns the gammas and covariance matrix 

    Returns
    -------
    dict 
    '''

    with open(path) as f:
    data = {}
    reader = csv.reader(f)
    for row in reader:
        if row[0] == 'gamma':
            data['gamma'] = reader.next()
        if row[0] == 'gammavcv':
            data['gammavcv'] = reader.next()
        if row[0] == 'residvcv':
            data['residvcv'] == reader.next()
    return data

def get_gammas(path, seed):
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

    data = read_csvv(path)
    np.random.seed(seed)

    data['gamma'] = multivariate_normal.rvs(data['gamma'], data['gammavcv'])

    return data


def get_weather(path):
    '''
    Precompute the values for the weather covariates
    '''





def pval_thing():
    '''
    Generate a list of pvals
    '''

    


def rebase1(ds, baseline):
    '''
    
    
    '''


def clipping(ds, clip_limit):
    '''

    '''


def goodmoney(ds):
    '''
    Some method to transform data according to goodmoney
    '''



def adaptation(ds, adaptation=None):
    ''''
    Some methods to update calcuation according to some inputs
    '''

def rescale(ds, scaling=None):
    '''
    Some methods to rescale the data
    '''

def rebase2(ds, baseline):
    '''
    Some method to rebase along dim2
    '''

def combine(ds):
    '''
    Not sure what this does
    '''


def costs(ds, *args):
    '''
    Some methods to account for costs
    '''

def aggregate(ds, level=None):
    '''
    Some level of resolution to aggregate to
    '''



