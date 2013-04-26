from sqlalchemy import create_engine, update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
import json

#establish connection to thedb
engine = create_engine('sqlite:///stream.db', echo=False)
Base = declarative_base(engine)

class Tweet(Base):
	__tablename__ = 'tweet'
	__table_args__ = {'autoload':True}

class User(Base):
	__tablename__ = 'user'
	__table_args__ = {'autoload':True}

#load all the field names
def loadSession():
	metadata = Base.metadata
	Session = sessionmaker(bind=engine)
	session = Session()
	return session

def isRT(tweet):
	if "RT @" in tweet:
		return True
	else:
		return False

def isMentions(mentions):
	mentions = json.loads(mentions)
	if(len(mentions) > 0):
		return True
	else:
		return False

def updateRTCount(mentions, Session):
	#convert mentions back into json
	mentions = json.loads(mentions)
	#if first mentioned user is a match, update user_retweet_count
	user_match = Session.query(User).filter(User.id == mentions[0]['id']).first()
	if (user_match):
		#increment the field by one
		user_match.user_retweet_count += 1
		Session.commit()
		#after update
		print "After RT Update: %s" % Session.query(User).filter(User.id == mentions[0]['id']).all()[0].user_retweet_count
		#print the ID
		print "Mention ID: %s" % mentions[0]['id']
		print "-----------------------------------"
	
def updateMentionCount(mentions, isRT, Session):
	#convert mentions back into json
	mentions = json.loads(mentions)
	#for each mention after first, check if user exists in DB
	if isRT:
		print "isRT True: %s" % len(mentions)
		i = 1
		while i < len(mentions):
			#if mentioned user is a match, update user_mention_count
			user_match = Session.query(User).filter(User.id == mentions[i]['id']).first()
			if(user_match):
				#increment the field by one
				user_match.user_mention_count += 1
				Session.commit()
				#after update
				print "After Mention Update: %s" % Session.query(User).filter(User.id == mentions[i]['id']).all()[0].user_mention_count
				#print the id
				print "Mention ID: %s" % mentions[i]['id']
			i += 1	
	else:
		print "isRT False: %s" % len(mentions)
		i = 0
		while i < len(mentions):
			#if mentioned user is a match, update user_mention_count
			user_match = Session.query(User).filter(User.id == mentions[i]['id']).first()
			if(user_match):
				#increment the field by one
				user_match.user_mention_count += 1
				Session.commit()
				#after update
				print "After Mention Update: %s" % Session.query(User).filter(User.id == mentions[i]['id']).all()[0].user_mention_count
				#print the id
				print "Mention ID: %s" % mentions[i]['id']
			i += 1	
	print "-----------------------------------"

def updateURLCount(urls,):


def updateTickerCount(url,):



if __name__ == '__main__':
	Session = loadSession()
	tweets = Session.query(Tweet).all()
	users = Session.query(User).all()
	
	#update RT count
	for tweet in tweets:
		if isRT(tweet.status_text):
			updateRTCount(tweet.status_mentions,Session)

	#update mention count
	for tweet in tweets:
		if isMentions(tweet.status_mentions):
			is_rt = isRT(tweet.status_text)
			updateMentionCount(tweet.status_mentions,is_rt,Session)

	#update URL count
	for tweet in tweets:
		print twee

	#update Ticker count
	#
