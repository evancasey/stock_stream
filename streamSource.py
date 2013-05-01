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

#updates user_source with 
def updateSource(Session, user_id, tweet_source):
	user = Session.query(User).filter(User.id == user_id).first()
	user.user_source = tweet_source
	Session.commit()

#checks if user_source has been updated
def isUpdated(Session, user_id):
	user = Session.query(User).filter(User.id == user_id).first()
	if user.user_source != "0":
		print "Source is Updated"
		return True
	else:
		print "Updating source..."
		return False

if __name__ == '__main__':
	Session = loadSession()
	users = Session.query(User).all()
	tweets = Session.query(Tweet).all()

	tweet_count = 0
	for tweet in tweets:
		tweet_count += 1
		print tweet_count
		if not isUpdated(Session, tweet.user_id):
			updateSource(Session, tweet.user_id, tweet.status_source)