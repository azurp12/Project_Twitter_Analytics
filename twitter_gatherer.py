#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 20 21:03:00 2020

@author: silviana amethyst

This file collects data from twitter, and saves it to a JSON file.  
It requires a file called `twitter_credentials.py` to be on the path (current
working directory is on the path), and that file must contain four variables:
    * acc_secret
    * acc_token
    * con_key
    * con_secret

This file writes a file called `twitter_data.json`.  Saving of this file is 
goverened by the Twitter developer license agreement.

Generally, it might be better to save via pickeling, but we wanted the students 
to deal with JSON in this part of the project.  This file is slightly fragile,
in that it uses the private data field, `_json`, since a Status object is not
natively JSON serializable.  
"""
#%% 
search = '#ActiBlizzwalkout'

#%%
import tweepy, json
# https://docs.tweepy.org/en/latest/api.html

#%%
from twitter_credentials import acc_secret,acc_token,con_key,con_secret

#Use tweepy.OAuthHandler to create an authentication using the given key and secret
auth = tweepy.OAuthHandler(consumer_key=con_key, consumer_secret=con_secret)
auth.set_access_token(acc_token, acc_secret)

#Connect to the Twitter API using the authentication
api = tweepy.API(auth)


#%%

def collect(param_id, num=100):
 
    tweet_list = []

    
    # get the newest 20
    new_tweets = api.user_timeline(\
                screen_name = param_id
                )
    tweet_list.extend(new_tweets)
    
    # then we loop to get more until we're done.
    while len(tweet_list) < num:
        try:
            # id of oldest tweet seen so far
            last_id = new_tweets[-1].id 

            # do the search using the id
            new_tweets = api.user_timeline(\
                screen_name = param_id,
                max_id=last_id
                )

            # add the data to our growing set
            tweet_list.extend(new_tweets)
            
        except tweepy.TweepError as e:
            print("Error", e)
            break
        else:
            if not new_tweets:
                print("Could not find any more tweets!")
                break
            
    return tweet_list

#%%
ff14 = collect("FF_XIV_EN", num=100)
WoW = collect('Warcraft', num= 100)
#%%

as_json = [x._json for x in ff14]
with open('ff14.json', 'w') as out:
    json.dump(as_json,out,indent=4)
    
as_json = [x._json for x in WoW]
with open('wow.json', 'w') as out:
    json.dump(as_json,out,indent=4)
