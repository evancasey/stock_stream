from ConfigParser import SafeConfigParser
from Stream import Stream
from SQL import SQL
import logging
import argparse
import re
import threading
import time
import sys
import os

class Collector():
    DEFAULT_CONFIG_FILE = "collector.config"
    
    def __init__(self):
        #setup logger
        self.setupLogger()
        
    def setupLogger(self):
        logging.basicConfig(filename="collector.log")
        self.logger = logging.getLogger('Collector')
        fh = logging.FileHandler("collector.log")
        formatter = logging.Formatter('[%(asctime)s]:')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.setLevel(logging.INFO)
        self.logger.info("Collector Started")
        

    def getConfig(self, file = DEFAULT_CONFIG_FILE):
        configParser = SafeConfigParser()
        if not configParser.read(file):
            print "Config file \"", file, "\" not found.\nExiting..."
            self.logger.error("Config file \"" + 
                              file + "\" not found.\nExiting...")
            exit(0)
        #check config file for missing DB options
        if "db_info" not in configParser.sections():
            print "Config filuser_ide missing database info.\nExiting..."
            self.logger.error("Config file missing database info.\nExiting...")
            exit(0)
        
        users = []
        try:
            self.logger.info("Parsing config file: ' " + file + " '")
            sections = configParser.sections(); sections.remove("db_info")
            #get db info
            db_info = {"db_host": configParser.get("db_info", "db_host"),
                       "db_user": configParser.get("db_info", "db_user"),
                       "db_pass": configParser.get("db_info", "db_pass"),}
            #test db con
            self.sql = SQL(db_info['db_host'], db_info['db_user'], db_info['db_pass'])
            #get all users in config file
            for section in sections:
                users.append(
                    {"name"        : section, 
                    "con_key"      : configParser.get(section, "con_key"),
                    "con_secret"   : configParser.get(section, "con_secret"),
                    "key"          : configParser.get(section, "key"),
                    "secret"       : configParser.get(section, "secret"),
                    "lists"        : configParser.get(section, "lists"),
                    "db"           : configParser.get(section, "db")}
                             )
                #test db
                self.sql.testDB(configParser.get(section, "db"))
            
        except Exception, e:
            print e, "\nPlease fix the config file.\nExiting..."
            self.logger.error(str(e) + "\nPlease fix the config file.\nExiting...")
            exit(0)
        return users
    
    def parseLists(self, lists):
        if lists.lower() == "none" or not lists:
            return None
        try:
            regex = re.compile(',+')
            
            lists = regex.split(lists.replace(" ", ""))
            lists = filter(None, lists)
            
            listOS = []
            for list in lists:
                listOS.append({'owner' : list[:list.index(":")],
                               'slug'  : list[list.index(":")+1:]})
                
            return listOS
        except IOError as e:
            err = "Error parsing lists"
            print err
            self.logger.error(err)
            exit(0)
            
    def parseArgList(self):
        #setup args parser
        parser = argparse.ArgumentParser(description='Collect tweets from Twitter')
        parser.add_argument('-c','--config', 
                            help='Path to config file. Default config file will'+
                                  ' not be used.', 
                            required=False)
        
        args = vars(parser.parse_args())
        #load configs
        if args['config']:
            config = self.getConfig(args['config'])
        else:
            config = self.getConfig()
        #get file to twitter id, if it exists
            
        self.start(config)
        
        
    def start(self, users):
        print "Starting..."
        #start Twitter stream
        streamThreads  = []
        streamBuffers  = []
        
        for user in users:
            sr = Stream(user['con_key'], user['con_secret'], 
                        user['key'], user['secret'], 
                        user['name'])
            
            #insert user list into db
            list = sr.getUserList(self.parseLists(user['lists']))
            
            self.sql.insert_into_userList(list, user['db'])
            
            #get buffer and run stream
            buff = sr.getTweetsBuffer()
	    if list is not None:
            	stream = sr.run(list)
	    else:
		  stream = sr.run(None)
            #add new buff and stream to list
            streamBuffers.append({'buff':buff, 'db':user['db']})
            streamThreads.append(stream)
            print "Started user: " + user['name']
            self.logger.info("Started user: " + user['name'])

        while True:
            try:
                for buffer in streamBuffers:
                    tweet = buffer['buff'].pop()
                    if not tweet:
                        time.sleep(1)
                    else:
                        self.sql.insert_into(buffer['db'], tweet)
            except KeyboardInterrupt:
                print "Exiting..."
                os._exit(0)

if __name__ == "__main__":
    Collector().parseArgList()