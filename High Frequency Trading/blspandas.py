'''
This module contains functions that are mainly wrappers for functions described at:
https://www.bd-econ.com/blsapi.html
'''

import pandas as pd
import requests
import json


def get_state_fips():
    '''
    Get a list of State FIPS Ids.
    Retuns a pandas dataframe of bls ids.
    '''

    state_fips = pd.read_html('https://www.nrcs.usda.gov/wps/portal/nrcs/detail/?cid=nrcs143_013696')[1]
    state_fips = state_fips[0:50]
    state_fips = state_fips.loc[:,('Name','FIPS')]

    return state_fips



def get_bls_id_dicts(bls_fips, prefix, suffix):
    '''
    Function to generate two dictionaries of BLS Ids and state names.
    Return two dictionaries like {'bls_id': 'state'}
    '''


    def create_bls_id(x, prefix, suffix):
        '''
        Function to be used with pandas to create a column of BLS query Ids from FIPS codes.
        Returns a single bls id.
        '''

        if x < 10: 
        
            x = str(x)
            
            x = '0'+ x
            
        x = str(x)
        
        x = str(prefix) + x + str(suffix)
        
        return x

    

    # Convert from float to integer
    bls_fips['FIPS'] = bls_fips['FIPS'].astype('int64')

    # Apply our function to create a new column with the BLS_ID
    # pass our prefix and suffix as args to the pandas apply function
    bls_fips['BLS_ID'] = bls_fips.FIPS.apply(create_bls_id,args=(prefix,suffix))

    # Convert the dataframe to a dictionary like:
    # {'bls_id': 'state'}
    bls_fips = bls_fips.loc[:,('BLS_ID','Name')].set_index('BLS_ID')

    # Pass the dataframe to a dictionary
    bls_dict = bls_fips.to_dict().get('Name')

    '''
    For some reason, the query to the BLS API only returned half the states requested.
    As a workaround, split the FIPS list in half and run the query twice.
    '''

    # Using items() + len() + list slicing 
    # Split dictionary by half 
    bls_dict1 = dict(list(bls_dict.items())[:len(bls_dict)//2]) 
    bls_dict2 = dict(list(bls_dict.items())[len(bls_dict)//2:])

    return bls_dict1,bls_dict2


def query_bls(series_dict, key, dates):
    '''
    Function to query the BLS API via a POST request.
    Returns a pandas dataframe.
    https://www.bd-econ.com/blsapi.html
    '''

    # The url for BLS API v2
    url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'

    # Start year and end year
    #dates = ('2019', '2020')


    # Specify json as content type to return
    headers = {'Content-type': 'application/json'}

    # Submit the list of series as data
    data = json.dumps({
        "seriesid": list(series_dict.keys()),
        "startyear": dates[0],
        "endyear": dates[1]})

    # POST request for the data
    p = requests.post(
        '{}{}'.format(url, key),
        headers=headers,
        data=data).json()['Results']['series']

    # Date index from first series
    date_list = [f"{i['year']}-{i['period'][1:]}-01" for i in p[0]['data']]

    # Empty dataframe to fill with values
    df = pd.DataFrame()

    # Build a pandas series from the API results, p
    for s in p:
        df[series_dict[s['seriesID']]] = pd.Series(
            index = pd.to_datetime(date_list),
            data = [i['value'] for i in s['data']]
            ).astype(float).iloc[::-1]

    return df


def clean_bls_data(df):
    '''
    Function for cleaning BLS data into a 'tidy' vertical dataset like:
    State | Date | Metric
    Returns pandas dataframe.
    '''

    # Melt the dataframe into vertical format.
    melted_df = pd.melt(df, id_vars=['Date'],
                       value_vars=df.columns[1:],
                       var_name='State',
                       value_name='Pct_Unemployed'
                       )

    return melted_df


def adjust_inflation(year):
    
    cpi_index = pd.read_csv('cpi_index.csv')
    
    cpi_2018 = cpi_index[cpi_index.Year == 2018][['CPI-U-RS Index']].values[0][0]
    
    cpi_year = cpi_index[cpi_index.Year == year][['CPI-U-RS Index']].values[0][0]
    
    cpi_ratio = cpi_2018 / cpi_year
    
    return cpi_ratio

