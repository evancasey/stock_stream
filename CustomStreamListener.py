#===================================================
# CSCI181: Social Network Analysis Final Project
#
# Captures tweets from the Twitter Streaming API 
#===================================================

import sys
import tweepy
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
)
import json

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

#setting up our models

#class to store individual tweet traits
class Tweet(Base):
	__tablename__ = 'tweet'

	id = Column(Integer, primary_key=True)
	status_text = Column(String)
	user_name = Column(Integer)
	status_is_retweeted = Column(Boolean)
	status_retweet_count = Column(Integer)
	status_created_at = Column(DateTime, index = True)
	status_source = Column(String)

#class to store individual user traits
class User(Base):
	__tablename__ = 'user'

	id = Column(Integer, primary_key=True)
	user_name = Column(String)
	user_time_zone = Column(String)

	user_follow_request_sent = Column(Boolean)
	user_statuses_count = Column(Integer)
	user_followers_count = Column(Integer)
	user_favourites_count = Column(Integer)
	user_listed_count = Column(Integer)
	user_mention_count = Column(Integer)
	user_retweet_count = Column(Integer)

#main streaming class
class CustomStreamListener(tweepy.StreamListener):

	def on_status(self, status):

		print "Storing Tweet...."
		
		t_store = Tweet(id = status.id,
						status_text = status.text,
						user_name = status.author.screen_name,
						status_is_retweeted = status.retweeted,
						status_retweet_count = status.retweet_count,
						status_created_at = status.created_at,
						status_source = status.source)

		Session.add(t_store)

		
		status_id = status.id
		user_id = status.author.id

		user_name = status.author.screen_name

		status_text = status.text.encode('utf-8') if status.text else "null"
		status_is_retweeted = status.retweeted
		status_retweet_count = status.retweet_count
		user_listed_count = status.author.listed_count
		user_follow_request_sent = status.author.follow_request_sent
		
		user_followers_count = status.author.favourites_count
		user_favourites_count = status.author.favourites_count
		user_statuses_count = status.author.statuses_count
		user_time_zone = status.author.time_zone.encode('utf-8') if status.author.time_zone else "null"
		status_created_at = status.created_at
		status_source = status.source.encode('utf-8') if status.source.encode else "null"

		try:
		 	 print status_text

		 	 print "Committing.."

		 	 Session.commit()

		 	 print "Printing out the rows"
		 	 
		 	 s = select([Tweet])
			 result = s.execute()
			 
			 for row in result:
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
	stream.filter(track=queryUsers)
