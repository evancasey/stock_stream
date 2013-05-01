from sqlalchemy import create_engine, update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
import json
import csv

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

if __name__ == '__main__':
	Session = loadSession()
	users = Session.query(User).all()

	outfile = open('mydump.csv', 'wb')
	outcsv = csv.writer(outfile)
	[ outcsv.writerow(curr.field_one, curr.field_two)  for curr in users ]
	# or maybe use outcsv.writerows(records)

	outfile.close()