'''
This file is an interface for a set of functions needed in impact forecasting
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


def do_socio_thing(ds, , path, SSP=None):
    ''' 
    Method to calculate ssp transformation
    '''

def do_gdp_thing(ds, path):
    '''
    Method to calc gdp climate interaction
    '''

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



