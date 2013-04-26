from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

#establish connection to thedb
engine = create_engine('sqlite:///stream.db', echo=True)
Base = declarative_base(engine)

class Tweet(Base):
	__tablename__ = 'tweet'
	__table_args__ = {'autoload':True}

#load all the field names
def loadSession():
	metadata = Base.metadata
	Session = sessionmaker(bind=engine)
	session = Session()
	return session

#querying the db
if __name__ == '__main__':
	session = loadSession()
	tweets = session.query(tweet).all()
	print res[1].status_text
