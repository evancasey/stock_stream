library(sqldf)

sqldf("SELECT count(*) FROM tweet", dbname = "stream.db")
