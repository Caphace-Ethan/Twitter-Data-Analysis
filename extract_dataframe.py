import json
import pandas as pd
from textblob import TextBlob

def read_json(json_file: str)->list:
    """
    json file reader to open and read json files into a list
    Args:
    -----
    json_file: str - path of a json file
    
    Returns
    -------
    length of the json file and a list of json
    """
    
    tweets_data = []
    for tweets in open(json_file,'r'):
        tweets_data.append(json.loads(tweets))
    
    print(len(tweets_data))
    return len(tweets_data), tweets_data


class TweetDfExtractor:
    """
    this function will parse tweets json into a pandas dataframe
    
    Return
    #------
    dataframe ////
    """
    def __init__(self, tweets_list):
        
        self.tweets_list = tweets_list

    # an example function
    def find_statuses_count(self)->list:
        statuses_count = []
        for element in self.tweets_list:
            try:
                if 'retweeted_status' in element:
                    favourite_count.append(element['retweeted_status']['user']['statuses_count'])

                else:
                    favourite_count.append(element['user']['statuses_count'])

            except Exceptions as e:
                print(e)

        return statuses_count
        
    def find_full_text(self)->list:
        text = []
        for element in self.tweets_list:
            if 'retweeted_status' in element:
                if 'extended_tweet' in element['retweeted_status']:
                    text.append(element['retweeted_status']['extended_tweet']['full_text'])
                else:
                    text.append(element['retweeted_status']['text'])
            else:
                try:
                    if 'extended_tweet' in element['quoted_status']:
                        text.append(element['quoted_status']['extended_tweet']['full_text'])
                    else:
                        text.append(element['quoted_status']['text'])
                except Exceptions as e:
                    print(e)
                    text.append(element['text'])

        return text
    
    def find_sentiments(self, text)->list:
        polarity = []
        for element in self.tweets_list:
            if 'retweeted_status' in element:
                if 'extended_tweet' in element['retweeted_status']:
                    polarity.append(element['retweeted_status']['extended_tweet']['polarity'])
                else:
                    polarity.append(element['retweeted_status']['polarity'])
            else:
                polarity.append(element['polarity'])
        return polarity, self.subjectivity

    def find_created_time(self)->list:
        created_at = []  # Initialize empty list
        for element in self.tweets_list:
            if 'retweeted_status' in element:
                created_at.append(element['retweeted_status']['created_at'])

            else:
                created_at.append(element['created_at'])

        return created_at

    def find_source(self)->list:
        source = []
        for element in self.tweets_list:
            try:
                if 'retweeted_status' in element:
                    source.append(element['retweeted_status']['source'])

                else:
                    source.append(element['source'])
            except Exceptions as e:
                print(e)
        return source

    def find_screen_name(self)->list:
        screen_name = []
        for element in self.tweets_list:
            try:
                if 'retweeted_status' in element:
                    screen_name.append(element['retweeted_status']['user']['screen_name'])

                else:
                    screen_name.append(element['user']['screen_name'])

            except Exceptions as e:
                print(e)
        return screen_name

    def find_followers_count(self)->list:
        followers_count = []
        for element in self.tweets_list:
            try:
                if 'retweeted_status' in element:
                    followers_count.append(element['retweeted_status']['user']['followers_count'])

                else:
                    followers_count.append(element['user']['followers_count'])

            except Exceptions as e:
                print(e)
        return followers_count

    def find_friends_count(self)->list:
        friends_count = []
        for element in self.tweets_list:
            try:
                if 'retweeted_status' in element:
                    friends_count.append(element['retweeted_status']['user']['friends_count'])

                else:
                    friends_count.append(element['user']['friends_count'])

            except Exceptions as e:
                print(e)
        return  friends_count


    def is_sensitive(self)->list:
        is_sensitive = []
        for element in self.tweets_list:
            if 'retweeted_status' in element:
                try:
                    is_sensitive.append(element['retweeted_status']['possibly_sensitive'])
                except Exceptions as e:
                    print(e)
                    is_sensit = None
                    is_sensitive.append(is_sensit)
            else:
                is_sensit = None
                is_sensitive.append(is_sensit)

        return is_sensitive

    def find_favourite_count(self)->list:
        favourite_count = []
        for element in self.tweets_list:
            try:
                if 'retweeted_status' in element:
                    favourite_count.append(element['retweeted_status']['user']['favourites_count'])

                else:
                    favourite_count.append(element['user']['favourites_count'])

            except Exceptions as e:
                print(e)

        return favourite_count
    
    def find_retweet_count(self)->list:
        retweet_count = []
        for element in self.tweets_list:
            try:
                if 'retweeted_status' in element:
                    retweet_count.append(element['retweeted_status']['retweet_count'])

                else:
                    retweet_count.append(element['retweet_count'])

            except Exceptions as e:
                print(e)

        return retweet_count

    def find_hashtags(self)->list:
        hashtags =[]
        for element in self.tweets_list:
            try:
                if 'retweeted_status' in element:
                    if 'extended_tweet' in element['retweeted_status']:
                        hashtags.append(element['retweeted_status']['extended_tweet']['entities']['hashtags'])
                    else:
                        hashtags.append(element['retweeted_status']['entities']['hashtags'])
                else:
                    hashtags.append(element['entities']['hashtags'])

            except Exceptions as e:
                print(e)

        return hashtags

    def find_lang(self)->list:
        lang = []
        for element in self.tweets_list:
            if 'lang' in element:
                lang.append(element['lang'])

            else:
                lang = None

        return lang

    def find_mentions(self)->list:
        mentions = []
        for element in self.tweets_list:
            try:
                if 'retweeted_status' in element:
                    if 'extended_tweet' in element['retweeted_status']:
                        mentions.append(element['retweeted_status']['extended_tweet']['entities']['user_mentions'][0])
                    else:
                        mentions.append(element['retweeted_status']['entities']['user_mentions'][0])
                else:
                    mentions.append(element['entities']['user_mentions'][0])

            except Exceptions as e:
                print(e)

        return mentions


    def find_location(self)->list:
        try:
            location = self.tweets_list['user']['location']
        except TypeError:
            location = ''
        
        return location

    
        
        
    def get_tweet_df(self, save=False)->pd.DataFrame:
        """required column to be generated you should be creative and add more features"""
        
        columns = ['created_at', 'source', 'original_text','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
            'original_author', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place']
        
        created_at = self.find_created_time()
        # print(created_at)
        source = self.find_source()
        text = self.find_full_text()
        polarity, subjectivity = self.find_sentiments(text)
        lang = self.find_lang()
        fav_count = self.find_favourite_count()
        retweet_count = self.find_retweet_count()
        screen_name = self.find_screen_name()
        follower_count = self.find_followers_count()
        friends_count = self.find_friends_count()
        sensitivity = self.is_sensitive()
        hashtags = self.find_hashtags()
        mentions = self.find_mentions()
        location = self.find_location()
        data = zip(created_at, source, text, polarity, subjectivity, lang, fav_count, retweet_count, screen_name, follower_count, friends_count, sensitivity, hashtags, mentions, location)
        df = pd.DataFrame(data=data, columns=columns)

        if save:
            df.to_csv('processed_tweet_data.csv', index=False)
            print('File Successfully Saved.!!!')
        
        return df

                
if __name__ == "__main__":
    # required column to be generated you should be creative and add more features
    columns = ['created_at', 'source', 'original_text','clean_text', 'sentiment','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
    'original_author', 'screen_count', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place', 'place_coord_boundaries']
    tweets_length, tweet_list = read_json("data/covid19.json")
    tweet = TweetDfExtractor(tweet_list)
    tweet_df = tweet.get_tweet_df()

    # use all defined functions to generate a dataframe with the specified columns above
    