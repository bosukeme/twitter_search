import snscrape.modules.twitter as sntwitter
from datetime import timedelta, date, datetime
import pandas as pd
from langdetect import detect
from pymongo import MongoClient
from dotenv import load_dotenv
import os 
from os.path import join, dirname
import twint 


from selenium import webdriver
from selenium.webdriver.chrome.options import Options
options=Options()
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('headless')

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from time import sleep



dotenv_path = join(dirname(__file__), '.env')

load_dotenv(dotenv_path)
MONGO_URL = os.environ.get('MONGO_URL')
client= MongoClient(MONGO_URL, connect=False)
#db = client.twitter_user_info
db = client.blovids

def process_twitter_details(keyword):
    start_date = str(date.today() + timedelta(days=-14))
    end_date = str(date.today() + timedelta(days=-7))
    
    usernames = []
    tweet_ids = []
    contents = []
    dates = []
    medium_links = []
    intext_links = []
    tweet_url = []
    for i,tweet in enumerate(sntwitter.TwitterSearchScraper(keyword  + " since:" + start_date + ' until:' +end_date).get_items()):
        if i > 10:
            break
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
    
    return df


def process_usernames(list_usernames):
    
    user_name_df = pd.DataFrame()
    user_id_list = []
    user_handle_list = []
    user_name_list = []
    user_bio_list = []
    user_profile_image_list = []

    for username in list_usernames:
        try:
            
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
            print()
            sleep(50)

        except:
            #print(username)
            user_id_list.append('NA')
            user_handle_list.append(username)
            user_name_list.append('NA')
            user_bio_list.append('NA')
            user_profile_image_list.append('NA')

    user_name_df['username'] = user_handle_list   
    user_name_df['twitter_ID'] = user_id_list  
    user_name_df['twitter_name'] = user_name_list  
    user_name_df['twitter_bio'] = user_bio_list  
    user_name_df['twitter_profile_image'] = user_profile_image_list

    
    #print(user_name_df)
    return user_name_df


def save_to_mongodb(merged_df):

    # Load in the instagram_user collection from MongoDB
    #twitter_medium_collections = db.twitter_medium_collections # similarly if 'testCollection' did not already exist, Mongo would create it
    med_collection = db.med_collection

    #cur = twitter_medium_collections.find() ##check the number before adding
    cur = med_collection.find()
    print('We had %s twitter_user entries at the start' % cur.count())
    
     ##search for the entities in the processed colection and store it as a list
    tweet_ids = list(med_collection.find({},{ "_id": 0, "tweet_id": 1})) 
    tweet_ids = list((val for dic in tweet_ids for val in dic.values()))

    
    #loop throup the handles, and add only new enteries
    for username, tweet_id, text, date, links, tweet_url, twitter_name, twitter_bio, twitter_profile_image in merged_df[['username', 'tweet_id', 'text', 'date', 'links', 'tweet_url', 'twitter_name', 'twitter_bio', 'twitter_profile_image']].itertuples(index=False):
        if tweet_id  not in tweet_ids:
            med_collection.insert_one({"username": username, "tweet_id":tweet_id, "text":text, "date":date, "links":links, "tweet_url":tweet_url, 'twitter_name':twitter_name, 'twitter_bio':twitter_bio, 'twitter_profile_image':twitter_profile_image}) ####save the df to the collection
    
    
  
    cur = med_collection.find() ##check the number after adding
    print('We have %s twitter_user entries at the end' % cur.count())
    

def search_db(usernames):
    med_collection = db.med_collection
    
    twitter_users=list(med_collection.find({},{ "_id": 0, "username": 1})) 
    twitter_users=list((val for dic in twitter_users for val in dic.values()))
    
    new_usernames = []
    for username in usernames:
        if username not in twitter_users:
            new_usernames.append(username)
    
    print("Len: ", len(new_usernames))
    return new_usernames


def get_unscraped_items(user_name_df):
    unscraped_users_df = user_name_df[user_name_df['twitter_name'] == 'NA']
    print(unscraped_users_df)
    unscraped_users_list = unscraped_users_df['username'].tolist()

    names, profile_photos, biographies, handle_list = process_unscrapped_users(unscraped_users_list)

    dff = pd.DataFrame()
    dff['twitter_name'] = names
    dff['twitter_bio'] = biographies
    dff['twitter_profile_image'] = profile_photos
    dff['username'] = handle_list

    return dff



def process_unscrapped_users(unscraped_users_list):
    names=[]
    followers_counts=[]
    profile_photos=[]
    biographies=[]

    for unscraped_user in unscraped_users_list:
        with webdriver.Chrome(options=options, executable_path="/usr/lib/chromium-browser/chromedriver") as driver:
            driver.wait = WebDriverWait(driver, 5)
            
            url='https://twitter.com/'+ unscraped_user
            driver.get(url) ##open the site
            driver.wait = WebDriverWait(driver, 5)

            try:
                ##getting the name
                class_path= "[class='css-1dbjc4n r-15d164r r-1g94qm0']"
                driver.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, class_path)))
                main=driver.find_elements_by_css_selector(class_path)
                sleep(5)
                name=[a.text.split('\n') for a in main][0][0]
                names.append(name)
            except:
                names.append('nil')

            
            try:
                ##getting the biography
                class_path= "[class='css-901oao r-hkyrab r-1qd0xha r-a023e6 r-16dba41 r-ad9z0x r-bcqeeo r-qvutc0']"
                #driver.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, class_path)))
                main=driver.find_elements_by_css_selector(class_path)
                sleep(5)
                biography=[a.text for a in main]
                biographies.append(biography)
            except:
                biographies.append('nil')

            
            try:
                ##get the profile picture
                driver.get("https://twitter.com/" + unscraped_user + "/photo")
                sleep(5)
                images = driver.find_elements_by_tag_name('img')
                for image in images:
                    photo= image.get_attribute('src')
                profile_photos.append(photo)
            except:
                profile_photos.append('nil')

       
    return  names, profile_photos, biographies, unscraped_users_list




def call_all_functions(keyword):
    usernames, tweet_ids, contents, dates, medium_links, intext_links, tweet_url = process_twitter_details(keyword)

    df = create_df(usernames, tweet_ids, contents, dates, medium_links, intext_links, tweet_url)

    list_usernames= df['username'].tolist()

    user_name_df = process_usernames(list_usernames)

    dff = get_unscraped_items(user_name_df)
    
    user_name_df = user_name_df[user_name_df['twitter_name'] != 'NA']

    join_dfs = pd.concat([user_name_df, dff], ignore_index= True)

    merged_df = df.merge(join_dfs, how='left', on='username')
    merged_df = merged_df.drop_duplicates( "tweet_id" , keep='first')
    merged_df = merged_df.reset_index(drop=True)
    
    new_usernames =search_db(list_usernames)

    save_to_mongodb(merged_df)

    name_link_dict=[]
    med_collection = db.med_collection
    for name in new_usernames:
        try:
            cur = twitter_med_collections.find_one({ "username": name})
            cur_link = cur.get('links')
            name_link_pair = {name : cur_link}
            name_link_dict.append(name_link_pair)
        except:
            pass    

    return name_link_dict


