"""
Docstring to TODO.
"""

import twitter_config as tc
import count_min_sketch
import json
import tweepy


class TwitterStream():

    def __init__(self):
        auth = tweepy.OAuthHandler(tc.TWITTER_APP_KEY,
                                   tc.TWITTER_APP_SECRET)
        auth.set_access_token(tc.TWITTER_TOKEN, tc.TWITTER_TOKEN_SECRET)
        api = tweepy.API(auth)

        self.cms = count_min_sketch.CountMinSketch(w=2000, d=10)

        stream_listener = TwitterStreamListener(sketch=self.cms)
        stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
        stream.filter(track=tc.TRACK_TERMS, async=True)


class TwitterStreamListener(tweepy.StreamListener):

    def __init__(self, sketch):
        super(TwitterStreamListener, self).__init__()
        self.sketch = sketch

    def on_status(self, status):
        if status.retweeted:
            return

        description = status.user.description
        loc = status.user.location
        text = status.text
        coords = status.coordinates
        geo = status.geo
        name = status.user.screen_name
        user_created = status.user.created_at
        followers = status.user.followers_count
        id_str = status.id_str
        created = status.created_at
        retweets = status.retweet_count

        for hash_obj in status.entities.get('hashtags'):
            hashtag = str(hash_obj.get('text')).lower()
            self.sketch.update(hashtag, retweets + 1)

        if self.sketch.totalCount() % 100 == 0:
            print('\n')
            print('total_count: ' + str(self.sketch.totalCount()))
            print('--------------- HEAVY HITTERS -------------')
            print(str(self.sketch.heavyHitters()))

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False
