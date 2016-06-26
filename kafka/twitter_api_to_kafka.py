import sys
import tweepy
import json
from kafka import KafkaProducer
import ConfigParser

# Load config
config = ConfigParser.ConfigParser()
config.read("./kafka.conf")

ckey = config.get('KafkaConfig', 'ckey')
csecret = config.get('KafkaConfig', 'csecret')
atoken = config.get('KafkaConfig', 'atoken')
asecret = config.get('KafkaConfig', 'asecret')
kafka_node_dns = config.get('KafkaConfig', 'kafka_node_dns')
topic = config.get('KafkaConfig', 'topic')

producer = KafkaProducer(bootstrap_servers = kafka_node_dns + ':9092')

class TweetStreamProducer(tweepy.StreamListener):

    def on_data(self, data):
        # Send raw data to kafka
        producer.send(topic, str(data))
        json_data = json.loads(data)
        print str(json_data['id']), " sent"
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
    # Filter to get English tweets only
    twitterStream.sample(languages=["en"])

if __name__ == "__main__":
    start()