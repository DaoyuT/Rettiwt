from flask import jsonify
from flask import render_template
from app import app
from cassandra.cluster import Cluster
import json
from collections import OrderedDict
import ConfigParser
from datetime import datetime
import pytz

config = ConfigParser.ConfigParser()
config.read("/home/ubuntu/frontend/app/flask.conf")

cluster = Cluster([config.get('FlaskConfig', 'cluster')])
session = cluster.connect(config.get('FlaskConfig', 'session'))

@app.route('/')

@app.route('/index')
def signin():
  return render_template("signin.html")

@app.route('/home_page/<uid>/<n>')
def home_page(uid, n):
  twitter_fmt = "%a %b %d %H:%M:%S %Z %Y"
  my_fmt = "%a %b %d %H:%M:%S"
  utc = pytz.timezone('UTC')
  local = pytz.timezone('America/New_York')
  query_for_tids = "SELECT tid FROM inboxes WHERE uid=%d LIMIT %d"
  query_for_tweets = "SELECT tweet FROM tweets WHERE tid=%d"
  query_for_name = "SELECT uname FROM usernames WHERE uid=%d"
  uname_response = session.execute(query_for_name % int(uid))
  username = "Daoyu Tu"
  if uname_response:
    username = uname_response[0].uname
  numOfTweet = int(n)
  if numOfTweet < 50:
    numOfTweet += 5
  tid_response = session.execute(query_for_tids % (int(uid), numOfTweet))
  tweet_list = []
  for val in tid_response:
    tweets_response = session.execute(query_for_tweets % int(val.tid))
    for x in tweets_response:
      tweet_list.append(x.tweet)
  json_response = []
  for x in tweet_list:
    tweet_json = json.loads(x, object_pairs_hook=OrderedDict)
    localized_time = utc.localize(datetime.strptime(tweet_json.get('created_at').replace('+0000', 'UTC'), twitter_fmt), is_dst=None).astimezone(local).strftime(my_fmt)
    tweet_simple = {'timestamp' : localized_time, 'text' : tweet_json.get('text'), 'authorName' : tweet_json.get('user').get('name'), 'authorScreenName' : tweet_json.get('user').get('screen_name')}
    hashtags = tweet_json.get('entities').get('hashtags')
    if hashtags:
      hashtag_str = []
      for hashtag in hashtags:
        hashtag_str.append(hashtag.get('text'))
      tweet_simple['hashtags'] = ", ".join(hashtag_str)
    json_response.append(tweet_simple)
  user = {'uid' : uid, 'uname' : username, 'numOfTweet' : numOfTweet}
  if json_response:
    return render_template("home_page.html", tweets=json_response, user=user, mode='home')
  else:
    return render_template("home_page.html", user=user, mode='home')

@app.route('/profile/<uid>/<n>')
def profile(uid, n):
  twitter_fmt = "%a %b %d %H:%M:%S %Z %Y"
  my_fmt = "%a %b %d %H:%M:%S"
  utc = pytz.timezone('UTC')
  local = pytz.timezone('America/New_York')
  query_for_tids = "SELECT tid FROM homepages WHERE uid=%d LIMIT %d"
  query_for_tweets = "SELECT tweet FROM tweets WHERE tid=%d"
  query_for_name = "SELECT uname FROM usernames WHERE uid=%d"
  uname_response = session.execute(query_for_name % int(uid))
  username = "Daoyu Tu"
  if uname_response:
    username = uname_response[0].uname
  numOfTweet = int(n)
  if numOfTweet < 50:
    numOfTweet += 5
  tid_response = session.execute(query_for_tids % (int(uid), numOfTweet))
  tweet_list = []
  for val in tid_response:
    tweets_response = session.execute(query_for_tweets % int(val.tid))
    for x in tweets_response:
      tweet_list.append(x.tweet)
  json_response = []
  for x in tweet_list:
    tweet_json = json.loads(x, object_pairs_hook=OrderedDict)
    localized_time = utc.localize(datetime.strptime(tweet_json.get('created_at').replace('+0000', 'UTC'), twitter_fmt), is_dst=None).astimezone(local).strftime(my_fmt)
    tweet_simple = {'timestamp' : localized_time, 'text' : tweet_json.get('text'), 'authorName' : username, 'authorScreenName' : tweet_json.get('user').get('screen_name')}
    hashtags = tweet_json.get('entities').get('hashtags')
    if hashtags:
      hashtag_str = []
      for hashtag in hashtags:
        hashtag_str.append(hashtag.get('text'))
      tweet_simple['hashtags'] = ", ".join(hashtag_str)
    json_response.append(tweet_simple)
  user = {'uid' : uid, 'uname' : username, 'numOfTweet' : numOfTweet}
  if json_response:
    return render_template("home_page.html", tweets=json_response, user=user, mode='profile')
  else:
    return render_template("home_page.html", user=user, mode='profile')