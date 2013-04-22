#===================================================
# CSCI181: Social Network Analysis Final Project
#
# Captures tweets from the Twitter Streaming API 
#===================================================

import sys
import tweepy
from dateutil import parser
from sqlalchemy import create_engine
from sqlalchemy.sql import select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import (
	Column,
	Integer,
	String,
	Boolean,
	DateTime,
	MetaData,
)
import json
import smtplib
import traceback

#Oauth verification for the Twitter Streaming API
CONSUMER_KEY = 'bN7Daq0GmSNA8Tbd7RFMeA'
CONSUMER_SECRET = 'T3QP1XIFYzaQpxzuyKgVjgn1HfYtS6Ftwr7cAlcf8G4'
ACCESS_TOKEN = '330015261-kRM2MzI4dXVNROv7Tv6Ok5LxaBabBLlX0kofiVZY'
ACCESS_TOKEN_SECRET = 'a9bKfBc6eG89SqW9FxIYfgzR2U2hqwDkHrGsWVU'

#using tweepy's built in oath handling
auth = tweepy.OAuthHandler(CONSUMER_KEY,CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN,ACCESS_TOKEN_SECRET)

#set up our local DB to store the tweets
db = create_engine('sqlite:///stream.db')
Base = declarative_base(bind=db)
Session = scoped_session(sessionmaker(db))
db.echo = True

#class to store individual tweet traits
class Tweet(Base):
	__tablename__ = 'tweet'

	id = Column(Integer, primary_key=True)
	status_text = Column(String)
	user_id = Column(Integer)
	user_follow_request_sent = Column(Boolean)
	status_is_retweeted = Column(Boolean)
	status_retweet_count = Column(Integer)
	status_original_tweet_id = Column(Integer)
	status_created_at = Column(String)
	status_source = Column(String)
	status_urls = Column(String)
	status_hashtags = Column(String)
	status_mentions = Column(String)
	status_is_retweet = Column(Boolean)

#class to store individual user traits
class User(Base):
	__tablename__ = 'user'

	id = Column(Integer, primary_key=True)
	user_name = Column(String)
	user_followers_count = Column(Integer)
	user_friends_count = Column(Integer)
	user_statuses_count = Column(Integer)
	user_favourites_count = Column(Integer)
	user_listed_count = Column(Integer)
	user_mention_count = Column(Integer)
	user_retweet_count = Column(Integer)

#creating our db
Base.metadata.create_all(db)

#main streaming class
class CustomStreamListener(tweepy.StreamListener):

	def on_status(self, status):

		print "Storing Tweet...."
		
		t_store = Tweet(id = status.id,
						status_text = status.text,
						user_id = status.author.id,
						user_follow_request_sent = status.author.follow_request_sent,
						status_is_retweeted = status.retweeted,
						status_retweet_count = status.retweet_count,
						status_original_tweet_id = status.retweeted_status.id if (status.retweet_count > 0) else 0,
						status_created_at = status.created_at,
						status_source = status.source,
						status_urls = json.dumps(status.entities['urls']),
						status_hashtags = json.dumps(status.entities['hashtags']),
						status_mentions = json.dumps(status.entities['user_mentions']),
						status_is_retweet = True if status.text[:2] == "RT" else False
						)

		#instance variables to use when updating

		u_store = Session.query(User).filter(User.id == status.author.id).first()
		if not u_store:
			u_store = User(id = status.author.id,
						   user_name = status.author.screen_name,
						   user_followers_count = status.user.followers_count,
						   user_friends_count = status.user.friends_count,
						   user_statuses_count = status.author.statuses_count,
						   user_favourites_count = status.author.favourites_count,
						   user_listed_count = status.author.listed_count,
						   user_mention_count = 0, #fill in later
						   user_retweet_count = 0 #fill in later
						   )

		Session.add(t_store)
		Session.add(u_store)
		
		try:
		 	 print status.text.encode('utf-8') if status.text else ""

		 	 print "Committing.."

		 	 Session.commit()

		 	 print "Printing out the rows"

		 	 t = select([Tweet])
			 t_result = t.execute()
			 
			 for row in t_result:
			    print row

		except Exception, e:
			print >> sys.stderr, 'Encountered Exception: ', e
			pass

	def on_error(self, status_code):

		print >> sys.stderr, 'Error...'
		return True #Don't kill the stream

	def on_timeout(self):

		print >> sys.stderr, 'Timeout...'
		return True #Don't kill the stream

if __name__ == '__main__':
	#using twitter handles as keywords (must be <400)
	queryUsers = []
	for line in open("users.txt", "r").readlines():
		queryUsers.append(line.strip())

	#using tickers as keywords (must be <400)
	queryTerms = []
	for line in open("tickers.txt", "r").readlines():
		queryTerms.append(line.strip())

	#call our main streaming handler
	listener=CustomStreamListener()
	stream=tweepy.streaming.Stream(auth,listener)
	stream.filter(track=queryTerms)
