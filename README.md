# YouTube Influencers

Builds a SQL database of YouTube channels and their subscriber numbers, topic keywords, etc.
Seeded from keywords.csv file and uses the official YouTube API (includes rate-limiting).

You can test with something small like `keywords_test.csv` first.

### High-Level Instructions

1. Install packages in requirements.txt to virtual environment.
2. Register for the YouTube API and update the API key variable accordingly in `yt_influencers.py`
3. (Optional)  Register for a Twilio free number and update the API key and phone numbers in `yt_influencers.py`.  This will just text you when the scrape fails, so it's not strictly necessary, but nice to have.  Comment out the innards of `text_me_then_quit()` if not using this.
4. Run the main script.  It will create a SQL database to hold the scrape results.

### Scrape Methodology Overview

Open up `keywords_test.csv` to follow along here.

The script will use the YouTube API to send queries that are combinations of keywords based on the entries in this file (actually, in `keywords.csv`, the `_test.csv` file is a smaller version for understanding what this script does and for doing a quick test run before kicking off the main scrape).

It will basically query every permutation along a row based on the cells that say `TRUE` in them.  For example, the first row will query `appliances reviews` and it will only search for channels containing those keywords.  Similarly, the third entry would result in a query like `best cooktops reviews` and would only search for videos with those keywords.  Note that whether `videos` or `channels` is specified, the script will always extract information about the *channel* to the database.

You can modify the CSV file however you desire based on what you're searching for.  If you'd like to use different modifications than "best" or "reviews" or "unboxing" or "tips," however, you'll need to modify the main Python script.  Should be pretty obvious where to make changes.

### Database Contents
The database will create two Tables, one for `Search` and one for `Channel`

`Search` contains content like `q=best%203d%20printers&type=video` and isn't really used other than tells the script what it's already done, so that the job can get interrupted and resume where it was, also avoiding scraping the same data twice.

`Channel` is the main information you're likely after, and contains information about a channel, such as channel title, description, channel keywords, thumbnail photo urls, country, total video views count, subscriber count, video count, if it's made for kids, and lastly potential contact emails.

Two fields are worth further explaining:
`channel keywords` is both a field that the YouTube API returns, as well as information the script adds as it goes.  For example, if a search term `appliances` resulted in a channel being found, that word would be added to its keywords list.
`potential contact emails` is not something YouTube gives you access to via the API, unfortunately.  However, many channel creators leave an email in their channel description (sometimes formatted like bob AT bob.com).  The script will look for these and, if found, save them here.

