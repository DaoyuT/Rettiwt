import sys
import tweepy
import json
from kafka import KafkaProducer

ckey = 'VpaEFU28gqdj2hWF6HWX4T7vm'
csecret = 'TFx9R6LSpJQAIgeypflR7osTbRqPuiYw8PM98ocT1NprL6IHTA'
atoken = '4167206710-o67meh2UwYNrKxKeJebeUsnDXJSkKZZQTCVIkE1'
asecret = 'NUeYqPt2DJCZFBj43naelDF650EKe6EfX9uddgfOtJGXO'

topic = "tweet_stream"
producer = KafkaProducer(bootstrap_servers='ec2-52-22-61-135.compute-1.amazonaws.com:9092')

class TweetStreamProducer(tweepy.StreamListener):

    def on_data(self, data):
        json_data = json.loads(data)
        print str(json_data['id']), " sent"
        producer.send(topic, str(data))
        return True

    def on_error(self, status_code):
        print >> sys.stderr, 'Encountered error with status code:', status_code
        return True # Don't kill the stream

    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        return True # Don't kill the stream

def start():
    auth = tweepy.OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)
    twitterStream = tweepy.streaming.Stream(auth, TweetStreamProducer())
    twitterStream.sample(languages=["en"])

if __name__ == "__main__":
    start()