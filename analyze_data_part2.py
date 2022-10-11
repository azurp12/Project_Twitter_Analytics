# -*- coding: utf-8 -*-
"""
Created on Wed Jul 28 15:35:37 2021

@author: perkc5965
"""

import numpy as np
import pandas as pd
import tweepy, json, pickle, re


pickle_off = open('twitter_data_large.txt', "rb")

temp = pickle.load(pickle_off)


# intialize the dataframe
tweets_df = pd.DataFrame()
original_tweets = pd.DataFrame()
# populate the dataframe
for tweet in temp:

    #pass tweet iformation into DF, catching exceptions passed by tweets without original retweet id's 
    try:
        tweets_df = tweets_df.append(pd.DataFrame([{'user_name': tweet.user.screen_name, 
                                               'user_location': tweet.user.location,
                                               'user_description': tweet.user.description,
                                               'text': tweet.full_text, 
                                               'favorite_count': tweet.favorite_count,
                                               'id': tweet.id,
                                               'retweet_count' : tweet.retweet_count,
                                               'orig_tweet_id': tweet.retweeted_status.id}]))
    except AttributeError:
        tweets_df = tweets_df.append(pd.DataFrame([{'user_name': tweet.user.screen_name, 
                                               'user_location': tweet.user.location,
                                               'user_description': tweet.user.description,
                                               'text': tweet.full_text, 
                                               'favorite_count': tweet.favorite_count,
                                               'id': tweet.id,
                                               'retweet_count' : tweet.retweet_count}]))
    
    
    #collect tweets that are the originals of retweets
    try:
        original_tweets = original_tweets.append(pd.DataFrame([{'id' : tweet.retweeted_status.id,
                                                  'text': tweet.retweeted_status.full_text,
                                                  'user_name': tweet.retweeted_status.author.screen_name}]))
    
    except AttributeError:
        pass
        
    tweets_df = tweets_df.reset_index(drop=True)






def most_orig_id(param_df):
    """
    intended parameter is variable: original_tweets DF

    Parameters
    ----------
    param_df : pandas data frame
        DF containing the original tweets of retweets.

    Returns
    -------
    count : Series
        Ordered Series by most reoccuring tweet.

    """
    
    count = param_df['id'].value_counts()
    
    return count







def parse_mentions(param_df):
    """
    

    Parameters
    ----------
    param_df : pandas data frame
        data frame containing tweets.

    Returns
    -------
    param_df : pandas data frame
        modified data frame with new column 'mentions'. Column is filled with
        numpy arrays containing the parsed mentions

    """
    
    param_df['mentions'] = np.array(param_df['text'].str.findall(r'(?:(?<=\s)|(?<=^))@.*?(?=\s|$)'))
    
    return param_df
    





def count_mentions(param_df):
    """
    *can only be used on a data frame passed through 'parse_mentions' function*
    
    iterates through numpy arrays in the column counting # of occurances
    
    Parameters
    ----------
    param_df : pandas data frame
        data frame that contains 'mentions' column.

    Returns
    -------
    count : Series
        Series containing # of occurances of mentions.

    """
    
    func_temp = pd.Series([])
    
    for ii in range(param_df.shape[0]):
        
        for jj in param_df.loc[ii, 'mentions']:

            func_temp= func_temp.append(pd.Series([jj]))            
    count = func_temp.value_counts()
    
    return count







def contains_bliz(param_df):
    """
    Parses 'user_description' for a word containing 'bliz'
    
    *calls convert_lower()* - changes all 'user_description' to lower-case for parsing

    Parameters
    ----------
    param_df : pandas data frame
        Data frame containing tweets.

    Returns
    -------
    param_df : pandas data frame
        returns a modified version of data frame to include 'contains_bliz' column
        (dtype: boolean)

    """
            
    param_df['user_description'] =\
        param_df['user_description'].apply(convert_lower)        
    param_df['contains_bliz'] = param_df['user_description'].str.contains('bliz')        
    return param_df






def convert_lower(param_str):
    ''' converts passed in str into lower case - returns modified string '''    
    result = param_str.lower()            
    return result





def blizzard_employees_df(param_df):
    """
    *param_df must be a DF passed through 'contains_blis()' function

    Parameters
    ----------
    param_df : pandas data fram
        data frame of tweets containing 'contains_bliz' column.

    Returns
    -------
    blizz_df : pandas data frame
        data frame containing only known Blizzard employees.

    """    
    
    blizz_df = param_df.loc[param_df['contains_bliz'] ==\
                            True, param_df.columns != 'contains_bliz'].copy()
        
    assert blizz_df.shape[0]>0, "Data set doesn\'t contain Blizzard employees"
    return blizz_df



def contains_rt(param_df):
    
    """ parses a tweet's text for retweet terminology, creates column 'is_retweet'
        (dtype: boolean)
    """
    
    
    param_df['is_retweet'] = param_df['text'].str.contains('RT @')    
    
    return param_df



def retweets_df(param_df):
    """
    *param_df must be passed through 'contains_rt()' first*
    

    Parameters
    ----------
    param_df : pandas data frame
        data frame of tweets containing 'contains_rt' column.

    Returns
    -------
    result_df : pandas data frame
        copied data frame containing only retweets.

    """
    result_df = param_df.loc[param_df['is_retweet']==\
                             True, param_df.columns != 'is_retweet'].copy()
        
    assert result_df.shape[0]>0, "Data set doesn\'t contain retweets"
    
    return result_df




def ratio_retweets_str(param_df):
    """
    calculates ratio of blizz employees to non-employees who have retweeted
    
    *param_df must be passed through 'contains_bliz()' function first*
        
    Parameters
    ----------
    param_df : pandas data frame
        data frame containing 'contains_bliz' column.

    Returns
    -------
    str
        Formatted str with a visualized ratio.

    """
    sum_bliz = param_df['contains_bliz'].sum()    
    non_bliz = param_df.shape[0] - sum_bliz   
    
    return f"The ratio of known Blizzard employee retweets compared to non-employee retweets is: {sum_bliz}/{non_bliz}"


def ratio_retweets_int(param_df):
    """
    *param_df must be passed through 'contains_bliz()' function first*

    Parameters
    ----------
    param_df : TYPE
        data frame containing 'contains_bliz' column.

    Returns
    -------
    sum_bliz : int
        sum of known blizzard employees.
    non_bliz : int
        sum of non-employees.

    """
    
    sum_bliz = param_df['contains_bliz'].sum()    
    non_bliz = param_df.shape[0] - sum_bliz   
    
    return sum_bliz, non_bliz

def str_info_most_orig(param_df, param_id):
    """
    creates string containing user and text of most retweeted tweet.
    
    *intended param_df is a drop duplicate result of 'most_orig_id()' function
    
    Parameters
    ----------
    param_df : pandas data frame
        data frame containing only unique original tweets.
    param_id : int
        tweet id of the most occurring original.

    Returns
    -------
    str
        formatted string displaying contents of the tweet.

    """
    
    tweet_text = param_df['text'].loc[param_df['id']== param_id] 
    
    tweet_user = param_df['user_name'].loc[param_df['id']== param_id]

    return f"user:{tweet_user[0]}\n text: {tweet_text[0]}"





def write_to_json(data):
    
    with open("analyzed_data.json", "w", encoding='utf-8') as outfile:
        json.dump(data, outfile, ensure_ascii= False, indent =4)
        
        
        
#setup main DF    
tweets_df = contains_bliz(tweets_df)
tweets_df = contains_rt(tweets_df)
tweets_df = parse_mentions(tweets_df)


#construct new DF out of original
retweets_temp = retweets_df(tweets_df)
count_ment = count_mentions(tweets_df)
count_orig=most_orig_id(original_tweets)
blizz_df = blizzard_employees_df(tweets_df)

#id of most_orig tweet
most_orig = count_orig.idxmax()

orig_tweets_nodupes = original_tweets.drop_duplicates().copy()

#most orig tweet string
most_orig_str = str_info_most_orig(orig_tweets_nodupes, most_orig)


#convert DF to dicts
tweets_df = tweets_df.to_dict()
retweets_temp = retweets_temp.to_dict()
count_ment = count_ment.to_dict()
count_orig= count_orig.to_dict()
blizz_df= blizz_df.to_dict()
orig_tweets_nodupes = orig_tweets_nodupes.to_dict()


package = {'tweets_df': tweets_df,
                    'orig_tweets': orig_tweets_nodupes,
                    'retweets_df': retweets_temp,
                    'count_ment': count_ment,
                    'count_orig': count_orig,
                    'bliz_employees': blizz_df,
                    'most_orig_str' : most_orig_str
                    }

# print(f"{orig_tweets_nodupes['text'].loc[orig_tweets_nodupes['id']== most_orig]} {orig_tweets_nodupes['user_name'].loc[orig_tweets_nodupes['id']== most_orig]}")

write_to_json(package)

