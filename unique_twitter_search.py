import snscrape.modules.twitter as sntwitter
from datetime import timedelta, date, datetime
import pandas as pd
from langdetect import detect
from pymongo import MongoClient
from dotenv import load_dotenv
import os 
from os.path import join, dirname
import twint 

from time import sleep


dotenv_path = join(dirname(__file__), '.env')

load_dotenv(dotenv_path)
MONGO_URL = os.environ.get('MONGO_URL')
client= MongoClient(MONGO_URL, connect=False)
db = client.twitter_user_info


def search_db_for_new_usernames():
    unique_twitter_medium_collections = db.unique_twitter_medium_collections


    all_usernames = list(unique_twitter_medium_collections.find({},{ "_id": 0, "username": 1})) 
    all_usernames = list((val for dic in all_usernames for val in dic.values()))

    
    return all_usernames


def process_twitter_details(keyword, all_usernames):
    start_date = str(date.today() + timedelta(days=-7))
    end_date = str(date.today() + timedelta(days=-0))
    
    usernames = []
    tweet_ids = []
    contents = []
    dates = []
    medium_links = []
    intext_links = []
    tweet_url = []
    for i,tweet in enumerate(sntwitter.TwitterSearchScraper(keyword  + " since:" + start_date + ' until:' +end_date).get_items()):
        if i > 5000:
            break
        if tweet.username not in all_usernames:
            print(tweet.username)
            usernames.append(tweet.username)
            tweet_ids.append(tweet.id)
            dates.append(tweet.date)
            contents.append(tweet.content)
            medium_links.append(tweet.outlinks)
            intext_links.append(tweet.tcooutlinks) 
            tweet_url.append(tweet.url)
        
    return usernames, tweet_ids, contents, dates, medium_links, intext_links, tweet_url



def create_df(usernames, tweet_ids, contents, dates, medium_links, intext_links, tweet_url):
    df = pd.DataFrame()

    df['username'] = usernames
    df['tweet_id'] = tweet_ids
    df['text'] = contents
    df['date'] = dates
    df['links'] = medium_links
    df['intext_link'] = intext_links
    df['tweet_url'] = tweet_url

    language=[]
    for a in df['text']:
        try:
            lang = detect(a)
            language.append(lang)
        except:
            language.append('nil')
    df['language'] = language
    df = df[df['language'] == 'en']
    df=df.reset_index(drop=True)
    print(df)
    return df


def process_usernames(list_usernames, df):
    
    for index, username in enumerate(list_usernames):
        try:
            user_name_df = pd.DataFrame()
            user_id_list = []
            user_handle_list = []
            user_name_list = []
            user_bio_list = []
            user_profile_image_list = []

            c = twint.Config()
            c.Username = username
            c.Store_object = True
            c.User_full = False
            c.Pandas =True
            twint.run.Lookup(c)
            user_df = twint.storage.panda.User_df.drop_duplicates(subset=['id'])
            user_id = list(user_df['id'])[0]
            user_name = list(user_df['name'])[0]
            user_bio = list(user_df['bio'])[0]
            user_profile_image = list(user_df['avatar'])[0]
            user_id_list.append(user_id)
            user_handle_list.append(username)
            user_name_list.append(user_name)
            user_bio_list.append(user_bio)
            user_profile_image_list.append(user_profile_image)

            user_name_df['username'] = user_handle_list   
            user_name_df['twitter_ID'] = user_id_list  
            user_name_df['twitter_name'] = user_name_list  
            user_name_df['twitter_bio'] = user_bio_list  
            user_name_df['twitter_profile_image'] = user_profile_image_list

            print()

            merged_df = df[index:index+1].merge(user_name_df, how='left', on='username')
            print(merged_df)

            save_unique_handles_to_mongodb(merged_df)
            sleep(60)


        except:
            pass


def save_unique_handles_to_mongodb(merged_df):

    # Load in the instagram_user collection from MongoDB
    unique_twitter_medium_collections = db.unique_twitter_medium_collections # similarly if 'testCollection' did not already exist, Mongo would create it
    
    cur = unique_twitter_medium_collections.find() ##check the number before adding
    print('We had %s twitter_user entries at the start' % cur.count())
    
     ##search for the entities in the processed colection and store it as a list
    usernames = list(unique_twitter_medium_collections.find({},{ "_id": 0, "username": 1})) 
    usernames = list((val for dic in usernames for val in dic.values()))

    
    #loop throup the handles, and add only new enteries
    for username, tweet_id, text, date, links, tweet_url, twitter_name, twitter_bio, twitter_profile_image in merged_df[['username', 'tweet_id', 'text', 'date', 'links', 'tweet_url', 'twitter_name', 'twitter_bio', 'twitter_profile_image']].itertuples(index=False):
        if username  not in usernames:
            unique_twitter_medium_collections.insert_one({"username": username, "tweet_id":tweet_id, "text":text, "date":date, "links":links, "tweet_url":tweet_url, 'twitter_name':twitter_name, 'twitter_bio':twitter_bio, 'twitter_profile_image':twitter_profile_image}) ####save the df to the collection
    
    
  
    cur = unique_twitter_medium_collections.find() ##check the number after adding
    print('We have %s twitter_user entries at the end' % cur.count())
    


def call_all_functions(keyword):
    all_usernames =  search_db_for_new_usernames()
    usernames, tweet_ids, contents, dates, medium_links, intext_links, tweet_url = process_twitter_details(keyword, all_usernames)

    df = create_df(usernames, tweet_ids, contents, dates, medium_links, intext_links, tweet_url)

    list_usernames= df['username'].tolist()

    user_name_df = process_usernames(list_usernames, df)

    return None


#call_all_functions("mediumarticles")
