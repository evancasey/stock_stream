library(sqldf)

sqldf("SELECT * FROM user
           WHERE user_followers_count >= 0 ORDER BY user_followers_count ASC", dbname = "stream.db")
