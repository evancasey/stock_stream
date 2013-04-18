"""
Captures tweets from the Twitter Streaming API using OAuth and stores them in a Postgres DB
"""

import sys
import tweepy

#Initialize empty list to store each queryChunk
queryArray = []

#Initialize an empty list to store all the queries
queryTerms = []
for line in open("SP500.txt", "r").readlines():
	queryTerms.append(line.strip())

#Break up queryTerms into chunks of 100 queries
i = 0
queryChunk = []
while i < len(queryTerms):
	queryChunk.append(queryTerms[i])
	i += 1
	if i % 400 == 0:
		queryArray.append(queryChunk)
		queryChunk = []
	if i == len(queryTerms) and queryChunk:
		queryArray.append(queryChunk)

print 'twitter_stream no threading'
print queryArray[0]
print queryArray[1]

#Needed for Oauth verification on the Twitter API
CONSUMER_KEY = 'bN7Daq0GmSNA8Tbd7RFMeA'
CONSUMER_SECRET = 'T3QP1XIFYzaQpxzuyKgVjgn1HfYtS6Ftwr7cAlcf8G4'

ACCESS_TOKEN = '330015261-kRM2MzI4dXVNROv7Tv6Ok5LxaBabBLlX0kofiVZY'
ACCESS_TOKEN_SECRET = 'a9bKfBc6eG89SqW9FxIYfgzR2U2hqwDkHrGsWVU'

auth = tweepy.OAuthHandler(CONSUMER_KEY,CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN,ACCESS_TOKEN_SECRET)

class CustomStreamListener(tweepy.StreamListener):

	def on_status(self, status):

		text = status.text.encode('utf-8') if status.text else "null"
		retweeted = status.retweeted
		retweet_count = status.retweet_count
		name = status.author.screen_name.encode('utf-8') if status.author.screen_name else "null"
		followers_count = status.author.favourites_count
		favourites_count = status.author.favourites_count
		author_created_at = status.author.created_at
		statuses_count = status.author.statuses_count
		time_zone = status.author.time_zone.encode('utf-8') if status.author.time_zone else "null"
		status_created_at = status.created_at
		source = status.source.encode('utf-8') if status.source.encode else "null"

		try:
		 	 print "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (text, retweeted, retweet_count, name, followers_count, 
		 	 	favourites_count, author_created_at, statuses_count, time_zone, status_created_at, source)


		except Exception, e:
			print >> sys.stderr, 'Encountered Exception: ', e
			pass

	def on_error(self, status_code):

		print >> sys.stderr, 'Error...'
		return True #Don't kill the stream

	def on_timeout(self):

		print >> sys.stderr, 'Timeout...'
		return True #Don't kill the stream

listener=CustomStreamListener()

stream=tweepy.streaming.Stream(auth,listener)
stream.filter(track=queryArray[0])