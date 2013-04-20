import urllib
import simplejson

def searchTweets(query):

	search = urllib.urlopen("http://search.twitter.com/search.json?q="+query)
	dict = simplejson.loads(search.read())

	with open("search_out.txt", "w") as out_file:
		for result in dict["results"]: # result is a list of dictionaries
			print "*",result["text"],"\n"
			out_file.write("Tweet: %s\n" % result["text"])

# we will search tweets about "fc liverpool" football team
searchTweets("@seekingalpha&rpp=400")