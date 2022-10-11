# -*- coding: utf-8 -*-
"""
Created on Mon Aug  9 02:25:28 2021

@author: perkc5965
"""
import numpy as np
import pandas as pd
import json




original_tweets_WoW = pd.DataFrame()
original_tweets_FF14 = pd.DataFrame()

#%%
''' Unpacking tweets from the twitter gatherer '''

with open('ff14.json') as tweet_data:
    temp = json.load(tweet_data)



for tweet in temp:

    original_tweets_FF14 = original_tweets_FF14.append\
        (pd.DataFrame([{'text': tweet['text'], \
                        'favorite_count': tweet['favorite_count'],\
                        'id': tweet['id'],\
                        'retweet_count' : tweet['retweet_count'],
                        'created_at' : tweet['created_at'],\
                        'brand': 'FF14'}]))

    

with open('wow.json') as tweet_data:
    temp = json.load(tweet_data)
for tweet in temp:

    original_tweets_WoW = original_tweets_WoW.append\
        (pd.DataFrame([{'text': tweet['text'], \
                        'favorite_count': tweet['favorite_count'],\
                        'id': tweet['id'],\
                        'retweet_count' : tweet['retweet_count'],
                        'created_at' : tweet['created_at'],\
                        'brand' : 'WoW'}]))
            
        
#%%


def get_activity(param_fav, param_retw):
    
    result = param_fav + param_retw
    
    return result

def compress_dates(param_date, increment):
    """
    

    Parameters
    ----------
    param_date : Str
        String containing a date format of 'mm/dd'.
    increment : int
        The increment to round dates to.

    Returns
    -------
    param_date : TYPE
        A rounded date that conforms to the increment.

    """
    
       
    param_date  = param_date.split('/')
    temp = int(param_date[1])
    temp= str(increment*round(temp/increment))
    param_date[1]=temp
    
    param_date = f"{param_date[0]}/{param_date[1]}"

    return param_date




#%%
''' Wrangle Data'''

original_tweets_FF14['created_at'] = original_tweets_FF14['created_at'].apply(pd.to_datetime)

original_tweets_FF14['total_activity'] = \
    original_tweets_FF14.apply(lambda x: get_activity(x.favorite_count, x.retweet_count), axis =1)
    
original_tweets_WoW['total_activity'] = \
    original_tweets_WoW.apply(lambda x: get_activity(x.favorite_count, x.retweet_count), axis =1)
    
    
    
    

original_tweets_FF14['created_at'] = original_tweets_FF14['created_at'].apply(pd.to_datetime)
original_tweets_FF14['created_at']= original_tweets_FF14['created_at'].dt.date

original_tweets_WoW['created_at'] = original_tweets_WoW['created_at'].apply(pd.to_datetime)
original_tweets_WoW['created_at']= original_tweets_WoW['created_at'].dt.date





original_tweets_FF14['created_at'] = \
    original_tweets_FF14['created_at'].apply(lambda x: x.strftime("%m/%d"))
    
original_tweets_WoW['created_at'] = \
    original_tweets_WoW['created_at'].apply(lambda x: x.strftime("%m/%d"))
    
    
    
    

original_tweets_WoW['compressed_dates2'] = \
    original_tweets_WoW['created_at'].apply(lambda x: compress_dates(x,2))
    
original_tweets_FF14['compressed_dates2'] = \
    original_tweets_FF14['created_at'].apply(lambda x: compress_dates(x,2))
    
    
    


original_tweets_WoW['compressed_dates5'] = \
    original_tweets_WoW['created_at'].apply(lambda x: compress_dates(x, 5))
    
original_tweets_FF14['compressed_dates5'] = \
    original_tweets_FF14['created_at'].apply(lambda x: compress_dates(x, 5))
    
    
    
    

merged_tweets = original_tweets_FF14.copy()
merged_tweets = merged_tweets.append(original_tweets_WoW)
merged_tweets.set_index(np.arange(0,merged_tweets.shape[0]),inplace = True)



#%%
''' Write data to csv file for transfer to ipynb file '''

def write_df(path, df):
   
          df.to_csv(path)
          
# write_df('wrangled_ff14.csv', original_tweets_FF14)
# write_df('wrangled_wow.csv', original_tweets_WoW)



