# Builds a SQL database of YouTube channels and their subscriber numbers, topic keywords, etc.
# Seeded from keywords.csv file and uses the official YouTube API (includes rate-limiting).

# NOTE: To start a clean crawl, delete database file!  (But don't really ever need to do this, just add to what's there)

import io
import re
import pickle
import time
import requests
import json
import csv
import sys

from time import sleep
from urllib.parse import quote as urlquote
from datetime import datetime

# DB stuff
from sqlalchemy import Column, Integer, String, Boolean, BigInteger
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

from twilio.rest import Client

#####################################
# CONSTANTS
#####################################
KEYWORD_CSV_FILE = "keywords.csv"
# KEYWORD_CSV_FILE = "keywords_test.csv"  # NOTE: just use for testing

# Keywords file column definitions (starting at 0)
KEYWORD_COL = 0
TYPE_COL = 1
BEST_COL = 2
REVIEWS_COL = 3
UNBOXING_COL = 4
TIPS_COL = 5
ADVICE_COL = 6

SEARCH_MODIFIERS = {
    "best ": {
        "pre_or_post": "pre",
        "col": 2
    },
    " tips": {
        "pre_or_post": "post",
        "col": 5
    },
    " reviews": {
        "pre_or_post": "post",
        "col": 3
    },
    " advice": {
        "pre_or_post": "post",
        "col": 6
    },
    " unboxing": {
        "pre_or_post": "post",
        "col": 4
    }
}

#####################################
# YouTube API
#####################################
API_BASE_URL = "https://www.googleapis.com/youtube/v3/"
API_KEY = 'xxx_GOOGLE_YOUTUBE_API_KEY_xxx'

# QUOTA
# 10,000/day limit
# 1/s limit
# 86,400 seconds / day
# So average rate would be 0.115 hits/s  (8.64 seconds/hit)
# TARGET_RATE = 0.115     # credits/s average (10,000credits / 86,400seconds)
TARGET_RATE = 0.112     # When this runs a real long time, it seems to go over, so fudge it down a bit, probably floating point precision error
SEARCH_SLEEP = 2.1
CHANNEL_SLEEP = 2.1     # These are just guesses designed to slow it down to reasonable rates.  Function later forces it to average rate.

# Twilio to text me on failures
# the following line needs your Twilio Account SID and Auth Token
twilio_client = Client("xxx_Twilio_Account_SID_xxx", "xxx_Twilio_Auth_Token_xxx")
to_num = "+1xxx_xxx_xxxx",    # Your phone
from_num= "+1xxx_xxx_xxxx"    # Twilio account phone

#####################################
# Helper Functions
#####################################

# Give it some default text because for most purposes, I just need to know it quit.
def text_me_then_quit(msg="YouTube scraping script aborted!"):
    twilio_client.messages.create(to=to_num, 
                       from_=from_num, 
                       body=msg)
    sys.exit()


def parse_keywords(text, search_keyword):
    # Split the keywords into comma-separated list, but need to keep the words between " " tokenized
    # This is more difficult than I would have imagined, and ended up writing a function for it.
    out = ""
    in_quote = False
    for c in text:
        # Iterate character by character
        if c == '"':
            # Toggle the state of if we're inside a double quote or not
            if not in_quote:
                in_quote = True
            else:
                in_quote = False
        elif c == ' ':
            # If a space, it depends on if we're in the middle of a quoted keyword or not
            if in_quote:
                out += ' '  # copy to output if inside quotes
            else:
                out += ','  # else make comma-separated
        else:
            # Copy to output
            out += c.lower()
    
    # Lastly, in case there are duplicate tags, remove them by creating a list, uniquifying, then converting back to string
    out_list = [ x for x in out.split(',') ]    # Make a list
    out_list.append(search_keyword)             # Add in the keyword from our search
    out_list = list(set(out_list))              # Uniquify duplicates
    if '' in out_list:
        out_list.remove('')
    out = ','.join(out_list)                    # Turn back to comma separated string
    
    return out


#####################################
# Main
#####################################

# Set up database
# engine = create_engine('sqlite:///youtube_crawl.db', echo = True) # prints commands to stdout
engine = create_engine('sqlite:///youtube_crawl.db')
Base = declarative_base()

# Define db table models here (inherits from Base class)
# Use singular names for tables as good convention
class Search(Base):
    __tablename__ = "Search"

    id = Column(Integer, primary_key=True)
    search = Column(String(length=128), unique=True)
    num_results = Column(Integer)
    # The "crawled" flag is just implicitly there already as we only save the row when
    # all sub-queries complete

class Channel(Base):
    __tablename__ = "Channel"

    id = Column(Integer, primary_key=True)
    # From youtube#searchResult response kind:
    # We'll enforce uniqueness on the channelID, which *is* unique
    channel_id = Column(String(length=64), unique=True)

    # Get these remaining values from the youtube#channel call:
    # Titles are not nec. unique, see Veritasium, but that's kinda an exception.
    title = Column(String(length=128))
    description = Column(String(length=1000))
    keywords = Column(String(length=512))       # Just a comma-separated string of keywords.  e.g. "appliance warranties,diy,cordless drill,fixing"
    thumb_default = Column(String(length=256))  # URL of thumbnail.  88 x 88px
    thumb_med = Column(String(length=256))      # URL of medium thumbnail.  240 x 240px
    thumb_high = Column(String(length=256))     # URL of high-res thumbnail.  800 x 800px
    published_at = Column(String(length=32))    # Using string instead of DateTime for simplicity (SQLite converts to string anyway)
    custom_url = Column(String(length=128))
    default_language = Column(String(length=16))
    country = Column(String(length=16))
    view_count = Column(BigInteger)
    subscriber_count = Column(BigInteger)
    video_count = Column(Integer)
    made_for_kids = Column(Boolean)
    potential_contact_emails = Column(String(length=128))   # Comma-separated list of potential contact emails parsed from description
   

# create all tables for those that haven't been created yet (uses engine as connectivity source)
# this won't lose any existing data
Base.metadata.create_all(engine)

# create session handle 
from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind = engine)
session = Session()

# Reopen a previously-saved API response to test with for now
# with open("response.pck", "rb") as fp:
#     response = pickle.load(fp)

# FIXME / TODO / BUGS:
#

print("Starting YouTube crawl.")
start = datetime.now()
elapsed_seconds = 0
credits_used = 0
channels_grabbed = 0

# Iterate on this ordered list of prefixes/postfixes first, and on inner loop the keywords.
# This will give us a breadth-first search instead of depth-first
for key, value in SEARCH_MODIFIERS.items():

    # Open the keywords to search file, need to re-open this file from the beginning with each new
    # keyword modifier to get the csv_reader iterator to reset.
    with open(KEYWORD_CSV_FILE, 'r') as f_keywords_r:
        csv_reader = csv.reader(f_keywords_r, delimiter=',')
        next(csv_reader, None)  # Skip headers

        # Go through the keywords file
        for row in csv_reader:
            # If this modifier (key) is true for this row, apply the search term
            # e.g. if key="best" then looks for a TRUE in the "best" column of the keywords file.
            if row[ value['col'] ] == "TRUE":
                # Determine if prefix or postfix to search query
                if value['pre_or_post'] == "pre":
                    # We url encode the search term, but not the actual q= or ? terms later, we don't want those escaped!
                    search_term = "q=" + urlquote(key + row[KEYWORD_COL].lower().strip())    # e.g. "q=best%20appliance%20warranties"
                elif value['pre_or_post'] == "post":
                    search_term = "q=" + urlquote(row[KEYWORD_COL].lower().strip() + key)    # e.g. "q=appliances%20reviews"
                else:
                    # Should not get here!
                    print("Error in pre/post logic!")
                    print(f"key: {key} value['col']: {value['col']} value['pre_or_post']: {value['pre_or_post']}")
                    print(f"row: {row}")
                    print("Aborting script.")
                    text_me_then_quit()

                # Modify search term to include the search type, "channels" or "videos"
                if row[TYPE_COL] == "videos":
                    search_term += "&type=video"                   # e.g. "q=best%20appliance%20warranties&type=videos"
                elif row[TYPE_COL] == "channels":
                    search_term += "&type=channel"                 # e.g. "q=appliances%20reviews&type=channels"
                else:
                    # Should not get here!
                    print("Error in video/channel type!")
                    print(f"keyword: {row[KEYWORD_COL].lower().strip()}")
                    print(f"row[TYPE_COL]: {row[TYPE_COL]}")
                    print("Aborting script.")
                    text_me_then_quit()
                
                # Check that we didn't already search this term
                already_searched = session.query(Search).filter(Search.search == search_term).first()     # Returns first result or None
                if already_searched:
                    # If we already searched this term, move on to the next keyword permutation
                    print(f"Already searched on search term: {search_term}")
                    continue
                
                # Otherwise, didn't search this term already... go on with this same row of keywords file...

                # Build rest of URL for the "search" command
                url = f'{API_BASE_URL}search?part=snippet&maxResults=50&{search_term}&key={API_KEY}'

                print(f"Searching on search term: {search_term}")

                # Send request and wait for response, up to 3 times
                for times in range(0, 3):
                    try:
                        response = requests.get(
                            url=url,
                            headers={'Accept': 'application/json', 'Accept-Encoding': 'gzip, deflate'}
                        )
                    except requests.exceptions.ConnectionError as e:
                        print(f"Connection error: {e}")
                        print("Can't connect, are you online?")
                        if times >= 2:
                            print("Fatal Error, too many retries.")
                            text_me_then_quit()
                        else:
                            sleep(10**(times+1))    # Sleeps 10 seconds, then 100 seconds (on last failure just aborts)
                            continue

                    json_response = response.json()

                    # Sanity check everything
                    if response.status_code == 200:
                        sleep(SEARCH_SLEEP)     # Got good results, delay here to not get rate limited by API server
                        credits_used += 100     # Searches cost 100 credits towards quota
                        break
                    else:
                        # If HTTP doesn't return 200, try again after waiting
                        print(f"Error, invalid status code.  Should be 200, got {response.status_code}")
                        if times >= 2:
                            print("Fatal Error, too many retries.")
                            text_me_then_quit()
                        else:
                            sleep(10**(times+1))    # Sleeps 10 seconds, then 100 seconds (on last failure just aborts)

                if json_response['kind'] != "youtube#searchListResponse":
                    print(f"Fatal Error, should be kind=youtube#searchListResponse, but got kind={json_response['kind']}")
                    text_me_then_quit()

                # Number of results to further search on
                num_results = len(json_response['items'])
                print(f"Got {num_results} results")
                
                for item in json_response['items']:
                    if item['kind'] != "youtube#searchResult":
                        print(f"Fatal Error, each item should be kind=youtube#searchResult, but got kind={item['kind']}")
                        text_me_then_quit()
                    
                    channel_id = item['snippet']['channelId']       # Get the channel.  Even if the search result was a video, it's the channel for that video.
                    
                    # Check if we've already searched for this channel_id before, if it's in the Channel table.
                    channel = session.query(Channel).filter(Channel.channel_id == channel_id).first()   # Returns first result or None
                    if channel:
                        # If we've already searched on this channel, we don't want to search again.
                        # But we do want to add that search term to its list of keywords
                        # channel.keywords is just a comma-separated string
                        keyword_list = [ x for x in channel.keywords.split(',') ]   # ['appliance', 'apple', 'bob is cool', 'whoah']
                        keyword_list.append(row[KEYWORD_COL].lower().strip())       # Don't include url-encoding, nor the search modifier
                        keyword_set = set(keyword_list)                             # Make sure these are unique
                        final_keywords = ','.join(keyword_set)                      # Just a comma separated string again, "apple,bob is cool,appliance,new,whoah"
                        channel.keywords = final_keywords
                        try:
                            session.commit()
                        except Exception as e:
                            print(f"Exception {e} when trying to update channel keywords.")
                            print(f"keyword_list: {keyword_list}")
                            print(f"final_keywords: {final_keywords}")
                            session.rollback()
                            text_me_then_quit()
                        finally:
                            print(f"  ..Added keywords for previously-saved channel {channel.title}") # Here using data from database record
                            session.close()     # Need to get channel.title before closing the session!  :-)

                    else:
                        # In this case, we haven't searched on this channel before.  Grab all data related to it.
                        url = f'{API_BASE_URL}channels?part=snippet%2CcontentDetails%2Cstatistics%2CbrandingSettings%2Cstatus&id={channel_id}&key={API_KEY}'

                        # Send request and wait for response, up to 3 times
                        for times in range(0, 3):
                            try:
                                response = requests.get(
                                    url=url,
                                    headers={'Accept': 'application/json', 'Accept-Encoding': 'gzip, deflate'}
                                )
                            except requests.exceptions.ConnectionError as e:
                                print(f"Connection error: {e}")
                                print("Can't connect, are you online?")
                                if times >= 2:
                                    print("Fatal Error, too many retries.")
                                    text_me_then_quit()
                                else:
                                    sleep(10**(times+1))    # Sleeps 10 seconds, then 100 seconds (on last failure just aborts)
                                    continue

                            json_response = response.json()

                            # Sanity check everything
                            if response.status_code == 200:
                                sleep(CHANNEL_SLEEP)    # Got good results, delay here to not get rate limited by API server
                                credits_used += 1       # Channel listings typically cost 1 credits towards quota
                                break
                            else:
                                # If HTTP doesn't return 200, try again after waiting
                                print(f"Error, invalid status code.  Should be 200, got {response.status_code}")
                                if times >= 2:
                                    print("Fatal Error, too many retries.")
                                    text_me_then_quit()
                                else:
                                    sleep(10**(times+1))    # Sleeps 10 seconds, then 100 seconds (on last failure just aborts)

                        if json_response['kind'] != "youtube#channelListResponse":
                            print(f"Fatal Error, should be kind=youtube#channelListResponse, but got kind={json_response['kind']}")
                            text_me_then_quit()

                        results_per_page = json_response['pageInfo']['resultsPerPage']
                        if results_per_page != 1:
                            print("")
                            print(f"Error, should only return 1 channel result for id, but returned {results_per_page} resultsPerPage.")
                            print(f"API url call was: {url}")
                            print("Continuing crawl.")
                            print("")
                            # Don't make this a fatal error, just move along to next channel
                            continue

                        # If we get to here, was a good #channel response, grab the data
                        # Already checked that there is only one list item.
                        snippet = json_response['items'][0]['snippet']
                        content_details = json_response['items'][0]['contentDetails']
                        statistics = json_response['items'][0]['statistics']
                        branding_settings = json_response['items'][0]['brandingSettings']
                        status = json_response['items'][0]['status']

                        # Set default values on certain keys in case they aren't populated, 
                        # otherwise will return a KeyError
                        # If these keys exist, won't overwrite them
                        snippet.setdefault('customUrl', '')
                        snippet.setdefault('defaultLanguage', '')
                        branding_settings['channel'].setdefault('keywords', '')
                        status.setdefault('madeForKids', False)

                        # Handle channel country, which could be in two different places (snippet, and possibly in brandSettings)
                        snippet.setdefault('country', '')
                        branding_settings['channel'].setdefault('country', '')
                        country = snippet['country'] or branding_settings['channel']['country']     # The OR will take if only one set, prioritize snippet if both set
                        
                        # Build keywords to tag this channel
                        creator_tagged_keywords = branding_settings['channel']['keywords']          # Space separated string on YouTube.  e.g. '"Jamie obrien" surfing "who is job" "weird waves" "river surfing" surf "the wedge" "jamie o\'brien" "how to surf" pipeline hawaii'
                        final_keywords = parse_keywords(creator_tagged_keywords, row[KEYWORD_COL].lower().strip())  # Parse the creator-tagged keywords, plus add our own search term
                        
                        # Extract potential contact emails from the channel description (no way to get through API)
                        email_at_char = re.findall(r'[\w\._%+-]+@[\w\.-]+', snippet['description'])             # bob@yahoo.com
                        email_at_spelled_out = re.findall(r'[\w\._%+-]+\sAT\s[\w\.-]+', snippet['description']) # bob AT yahoo.com
                        email_list = email_at_char + email_at_spelled_out                                       # combine both lists
                        email_list = [ x.lower() for x in email_list ]      # lowercase all
                        email_list = list(set(email_list))                  # uniquify duplicates
                        potential_contact_emails = ','.join(email_list)     # Make a comma-separated string

                        # Create a new channel for DB
                        new_channel = Channel(
                            channel_id=channel_id,
                            title=snippet['title'],
                            description=snippet['description'],
                            keywords=final_keywords,
                            thumb_default=snippet['thumbnails']['default']['url'],
                            thumb_med=snippet['thumbnails']['medium']['url'],
                            thumb_high=snippet['thumbnails']['high']['url'],
                            published_at=snippet['publishedAt'],
                            custom_url=snippet['customUrl'],
                            default_language=snippet['defaultLanguage'],
                            country=country,
                            view_count=int(statistics['viewCount']),     # YT API returns as string
                            subscriber_count=int(statistics['subscriberCount']),
                            video_count=int(statistics['videoCount']),
                            made_for_kids=status['madeForKids'],
                            potential_contact_emails=potential_contact_emails
                        )

                        # Add the channel to the DB
                        session.add(new_channel)
                        try:
                            session.commit()
                        except Exception as e:
                            print(f"Exception {e} when trying to add channel.")
                            session.rollback()
                            text_me_then_quit()
                        finally:
                            session.close()
                            channels_grabbed += 1
                            print(f"  Saved results for channel {snippet['title']}")

                # If we get here, we've completed the (50 max) searches on each channel
                # for that particular modified keyword search.  Save progress to Search table.
                completed_search = Search(
                    search=search_term,
                    num_results=num_results
                )
                
                # Add to Search progress table
                session.add(completed_search)
                try:
                    session.commit()
                except Exception as e:
                    print(f"Exception {e} when trying to add completed search for: {search_term}")
                    session.rollback()
                    text_me_then_quit()
                finally:
                    session.close()
                    print(f"Saved results for search term: {search_term}\n")

                # Sleep here so that we get down to the average crawl rate
                while True:
                    now = datetime.now()

                    # After 24 hours, (86,400 seconds), the datetime delta object rolls over
                    # Need to handle that case
                    if (now - start).seconds < elapsed_seconds:
                        # If the new value of the time delta gets smaller than the last iteration,
                        # that shouldn't happen, and we've rolled over.  Need to reset the deltas.
                        start = datetime.now()
                        credits_used = 0        # restart credit count too
                        sleep(1)                # don't divide by 0
                        now = datetime.now()
    
                    elapsed_seconds = (now - start).seconds     # int
                    cur_rate = credits_used / elapsed_seconds

                    if cur_rate > TARGET_RATE:
                        # Going too fast, need to delay
                        # Print rate at fixed width of 5 digits with 3 decimal places, and credits used as fixed width of 7 digits
                        print("... sleep 1m, rate={0:5.3f} (target={1:.3f}).  elapsed_seconds={2:7d}.  credits_used(24 hrs)={3:7d}.  channels_grabbed(this run)={4:7d} ...".format(cur_rate, TARGET_RATE, elapsed_seconds, credits_used, channels_grabbed))
                        sleep(60)
                    else:
                        # Can go again
                        break

print(f"YouTube crawl complete!  Grabbed {channels_grabbed} channels in this run.")