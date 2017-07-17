'''
Created on Oct 21, 2015

@author: geofurb
'''
from datetime import datetime
import pytz
import json

tweet_format = 'twitter'


# If properly included, return the tweet ID
def getTweetID(tweet) :
    if 'id' in tweet and \
    tweet['id'] is not None :
        return tweet['id']
    else :
        return None

def getDate(tweet) :
    if 'created_at' in tweet and \
    tweet['created_at'] is not None :
        return tweet['created_at']
    else :
        return None


def getInfluencers(tweet):
    """ Get all the influencers from this tweet
    
        returns : tweeter(int), influencers(list of ints)    
    """
    
    # Get the tweeter
    tweeter = getUserID(tweet)
    influencers = set()
    
    # Nothing to do if we couldn't get the tweeter
    if tweeter is None :
        return None, []
    
    # Add person they're replying to
    influencers.add(getReplyID(tweet))
    
    # Add person they retweeted
    influencers.add(getRetweetID(tweet))
    
    # Add person they quoted
    influencers.add(getQuotedUserID(tweet))
    
    # Add mentions
    influencers.update(getUserMentions(tweet))
  
    influencers.discard(tweeter)    
    influencers.discard(None)
    
    # Return our tweeter and their influencers
    return tweeter, list(influencers)

def getRetweetInfluencers(tweet) :
    """ Get the ReTweet influencers from this tweet 
    
        returns : tweeter(int), influencers(list of ints)
    """

    # Get the tweeter
    tweeter = getUserID(tweet)
    influencers = []
    
    # Nothing to do if we couldn't get the tweeter
    if tweeter is None :
        return None, influencers
    
    # Add person they retweeted
    retweeter = getRetweetID(tweet)
    if retweeter is not None and \
    retweeter != tweeter:
        influencers.append(retweeter)
    
    # Return our tweeter and their influencers
    return tweeter, influencers

def getReplyInfluencers(tweet) :
    """ Get the reply influencers from this tweet 
    
        returns : tweeter(int), influencers(list of ints)
    """    
    # Get the tweeter
    tweeter = getUserID(tweet)
    influencers = []
    
    # Nothing to do if we couldn't get the tweeter
    if tweeter is None :
        return None, influencers
    
    # Add person they're replying to
    replier = getReplyID(tweet)
    if replier is not None and \
    replier != tweeter :
        influencers.append(replier)
    
    # Return our tweeter and their influencers
    return tweeter, influencers

def getQuoteInfluencers(tweet) :
    """ Get the quote influencers from this tweet
    
        /!\ the retweet influencers are exculded from the list
    
        returns : tweeter(int), influencers(list of ints)
    """    
    # Get the tweeter
    tweeter = getUserID(tweet)
    influencers = set()
    
    # Nothing to do if we couldn't get the tweeter
    if tweeter is None :
        return None, []
    
    influencers.add(getQuotedUserID(tweet))
    influencers.discard(getRetweetID(tweet))
    influencers.discard(tweeter)    
    influencers.discard(None)
    
    # Return our tweeter and their influencers
    return tweeter, list(influencers)

def getMentionInfluencers(tweet) :
    """ Get the mentioned influencers from this tweet.
        
        /!\ retweeted, quoted and replied users are excluded from the list
    
        returns : tweeter(int), influencers(list of ints)
    """        
    # Get the tweeter
    tweeter = getUserID(tweet)
    influencers = set()
    
    # Nothing to do if we couldn't get the tweeter
    if tweeter is None :
        return None, []
    
    # Add mentions
    influencers.update(getUserMentions(tweet))
    
    influencers.discard(getRetweetID(tweet))
    influencers.discard(getReplyID(tweet))
    influencers.discard(getQuotedUserID(tweet))
    influencers.discard(tweeter)
    influencers.discard(None)
    
    # Return our tweeter and their influencers
    return tweeter, list(influencers)

# If properly included, return the tweeter's ID
def getUserID(tweet) :
    if 'user' in tweet and \
    tweet['user'] is not None and \
    'id' in tweet['user'] and \
    tweet['user']['id'] is not None :
        return tweet['user']['id']
    else :
        return None
    
# If properly included, return the tweeter's screen name
def getScreenName(tweet) :
    if 'user' in tweet and \
    tweet['user'] is not None and \
    'screen_name' in tweet['user'] and \
    tweet['user']['screen_name'] is not None :
        return tweet['user']['screen_name']
    else :
        return None
    
def getRetweetID(tweet):
    """ If properly included, get the retweet source user ID"""
    
    if 'retweeted_status' in tweet and \
    tweet['retweeted_status'] is not None and \
    'user' in tweet['retweeted_status'] and \
    tweet['retweeted_status']['user'] is not None and \
    'id' in tweet['retweeted_status']['user'] and \
    tweet['retweeted_status']['user']['id'] is not None :
        return tweet['retweeted_status']['user']['id']
    else :
        return None
    
def getRetweetTweetID(tweet):   
    """ If properly included, get the tweet ID of the retweeted tweet"""
    
    if 'retweeted_status' in tweet and \
    tweet['retweeted_status'] is not None:
        return getTweetID(tweet['retweeted_status'])        
    else :
        return None
        
def getReplyID(tweet):
    """ If properly included, get the ID of the user the tweet replies to """
    if 'in_reply_to_user_id' in tweet and \
    tweet['in_reply_to_user_id'] is not None :
        return tweet['in_reply_to_user_id']
    else :
        return None
        

def getUserMentions(tweet):
    """ If properly included, get the IDs of all user mentions, 
        including retweeted and replied users """
        
    mentions = []
    if 'entities' in tweet and \
    tweet['entities'] is not None and \
    'user_mentions' in tweet['entities'] :
        for mention in tweet['entities']['user_mentions'] :
            if 'id' in mention and\
            mention['id'] is not None :
                mentions.append(mention['id'])
    return mentions
    
def getQuotedUserID(tweet):
    """ If properly included, get the ID of the user the tweet is quoting"""
    
    if 'quoted_status' in tweet and \
    tweet['quoted_status'] is not None and \
    'user' in tweet['quoted_status'] and \
    tweet['quoted_status']['user'] is not None and \
    'id' in tweet['quoted_status']['user'] and \
    tweet['quoted_status']['user']['id'] is not None :
        return tweet['quoted_status']['user']['id']
    else :
        return None

# If properly included, return screen names to each ID
def getScreennames(userlist) :
    phonebook = dict()
    for user in userlist :
        if 'screen_name' in user and \
        user['screen_name'] is not None and \
        'id' in user and user['id'] is not None :
            phonebook[str(user['id'])] = '@' + user['screen_name']
        elif 'id' in user and user['id'] is not None :
            phonebook[str(user['id'])] = '@#######'
    return phonebook

# If properly included, return followers of each user by ID
def getFollowers(userlist) :
    phonebook = dict()
    for user in userlist :
        if 'followers_count' in user and \
        user['followers_count'] is not None and \
        'id' in user and user['id'] is not None :
            phonebook[str(user['id'])] = user['followers_count']
        elif 'id' in user and user['id'] is not None :
            phonebook[str(user['id'])] = 0
    return phonebook

# If included, read out tweet coordinates
def getTweetCoords(tweet) :
    if 'coordinates' in tweet and \
        tweet['coordinates'] is not None :
        return tweet['coordinates']
    else :
        return None

# If included, read out tweet place
def getTweetPlace(tweet) :
    if 'place' in tweet and \
        tweet['place'] is not None :
        return tweet['place']
    else :
        return None

def getTweetPlaceFullname(tweet):
    """ If included, read out tweet full name place """
    
    if 'place' in tweet and \
        tweet['place'] is not None and \
        'full_name' in tweet['place'] :
            return tweet['place']['full_name']
    else :
        return None

# Get the user's self-supplied location from their user profile entity in this tweet
def getTweetUserLocation(tweet):
    """ If included, read the user from the tweet and return their self-supplied location"""
    
    if 'user' in tweet and \
        tweet['user'] is not None and \
        'location' in tweet['user'] :
        return tweet['user']['location']
    else :
        return None

# Where available, get users' locations
def compileLocationInfo(userlist) :
    '''
    Compile location info on a user from their Twitter profile. The idea
    here is to grab it all so we can write a thorough stalking method later.
    '''
    locations = dict()
    for user in userlist :
        
        if 'geo_enabled' in user and \
            user['geo_enabled'] is not None :            # Read the user's latest tweet and get GPS if there
            # Read latest tweet for location info
            locations[user]['geo_enabled'] = user['geo_enabled']
        else :
            locations[user]['geo_enabled'] = None
        
        # Check for location info in latest tweet
        if 'status' in user and \
            user['status'] is not None :
            tweet = user['status']
            
            # Actual location of latest tweet
            locations[user]['coordinates'] = getTweetCoords(tweet)
                
            # Place related to latest tweet
            locations[user]['place'] = getTweetPlace(tweet)
        
        # Check for a user-supplied location
        if 'location' in user and \
            user['location'] is not None and \
            user['location'] is not '' :
            # Parse the location
            locations[user]['location'] = user['location']
        else :
            locations[user]['location'] = None
            
        # Check to see if time-zone info is available
        if 'time_zone' in user and \
            user['time_zone'] is not None :
            locations[user]['time_zone'] = user['time_zone']
        else :
            locations[user]['time_zone'] = None
        
        # Check to see if UTC offset info is available
        if 'utc_offset' in user and \
            user['utc_offset'] is not None :
            # Infer time zone from UTC offset for longitude
            locations[user]['utc_offset'] = user['utc_offset']
        else :
            locations[user]['utc_offset'] = None
    return locations

# Get user time zone
def getTimezone(tweet) :
    # Check to see if time-zone info is available
    if 'user' in tweet and \
        tweet['user'] is not None and \
        'time_zone' in tweet['user'] and \
        tweet['user']['time_zone'] is not None :
        return tweet['user']['time_zone']
    else :
        return None

# Get UTC clock offset
def getClockOffset(tweet) :
    # Check to see if time-zone info is available
    if 'user' in tweet and \
        tweet['user'] is not None and \
        'utc_offset' in tweet['user'] and \
        tweet['user']['utc_offset'] is not None :
        return tweet['user']['utc_offset']
    else :
        return None

# Lazily stalk a bunch of users; don't expend much effort
def getWeakLoc(userlist) :
    locs = compileLocationInfo(userlist)
    locations = dict()
    for user in userlist :
        # To-do
        pass
    return locations

# If properly included, return the tweet text
def getTweetText(tweet) :
    if 'text' in tweet and \
    tweet['text'] is not None :
        return tweet['text']
    else :
        return None
  
# If properly included, get the retweeted text
def getRetweetedText(tweet) :
    if 'retweeted_status' in tweet and \
    tweet['retweeted_status'] is not None and \
    'text' in tweet['retweeted_status'] and \
    tweet['retweeted_status']['text'] is not None :
        return tweet['retweeted_status']['text']
    else :
        return None
  
# If properly included, get the hashtags
def getHashtags(tweet) :
    hashtags = []
    if 'entities' in tweet and \
    tweet['entities'] is not None and \
    'hashtags' in tweet['entities'] :
        for hashtag in tweet['entities']['hashtags'] :
            if 'text' in hashtag and\
            hashtag['text'] is not None :
                hashtags.append(hashtag['text'])
    return hashtags

# If properly included, get the hashtags
def getURLs(tweet) :
    urls = []
    if 'entities' in tweet and tweet['entities'] is not None and \
    'urls' in tweet['entities'] :
        for url in tweet['entities']['urls'] :
            if 'expanded_url' in url and url['expanded_url'] is not None:
                urls.append(url['expanded_url'])
    return urls
    
# If properly included, get Twitter client used to create this tweet
def getSource(tweet) :
    if 'source' in tweet :
        return tweet['source']
    else :
        return None

def getTimeStamp(tweet, timezone='US/Eastern'):
    """If properly included, get the time stamp of the tweet
    from the 'created_at' field
    
    returns a datetime object converted to US/Eastern time zone, 
    unless specified differently
    
    """
    
    if 'created_at' in tweet and tweet['created_at'] is not None:
        try:
            timestamp = datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC)
            return timestamp.astimezone(pytz.timezone(timezone))
        except ValueError:
            return None
    else:
        return None


def getTweetIDtoTimestampDict(tweets_filenames, timezone='US/Eastern'):
    """ Returns a dictonary with
        keys -> tweet_id
        values -> datetime object
        
        Warning: tweet_ids are unique but not datetimes
        
        Parameters:
        -----------
        tweets_filenames : list of tweets filenames
    """

    timestamps_dict = dict()
    #read timestamps
    for file in tweets_filenames:
        with open(file, 'r') as tweets_file:
            for line in tweets_file:
                tweet = json.loads(line)
                tweet_id = getTweetID(tweet)
                timestamp = getTimeStamp(tweet, timezone=timezone)
                
                if tweet_id is not None and timestamp is not None:
                    timestamps_dict[tweet_id] = timestamp
            
    return timestamps_dict