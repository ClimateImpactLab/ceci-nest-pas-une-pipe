
import os
import itertools
import pandas as pd

inpaths = (
    '../playground/csv2/global-csvs-v2.0/hierid/' +
    'global_tasmax-over-95F_{rcp}_{per}_{rel}_days-over-95F_percentiles.csv')

outpaths = (
    '../playground/csv2/cities-v2.0/' +
    'city-summary_tasmax-over-95F-{city}.csv')

cities = [
    ('India-NewDelhi', 'IND.10.121.371'),
    ('USA-WashingtonDC', 'USA.9.317'), 
    #('USA-PhoenixAZ', 'USA.3.101')
    ]

rcps = ['rcp45', 'rcp85']
pers = ['1986-2005', '2020-2039', '2040-2059', '2080-2099']
rels = ['absolute', 'change-from-hist']

index_names = ['rcp', 'per', 'rel']
keys = list(itertools.product(rcps, pers, rels))
formatters = list(map(lambda val: dict(zip(index_names, val)), keys))

bigdf = pd.concat([
    pd.read_csv(inpaths.format(**val), index_col=0) for val in formatters],
    keys=pd.MultiIndex.from_tuples(keys, names=['rcp', 'per', 'rel']),
    axis=0)


for city, hierid in cities:
    fp = outpaths.format(city=city)
    if not os.path.isdir(os.path.dirname(fp)):
        os.makedirs(os.path.dirname(fp))

    bigdf.xs(hierid, level='hierid').to_csv(fp)
