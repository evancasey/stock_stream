library(sqldf)
library(ggplot2)

df <- sqldf("SELECT user_name, user_statuses_count, user_friends_count, user_followers_count FROM user 
           WHERE user_followers_count >= 0", dbname = "stream.db")

qplot(df$user_name, data = df, geom='histogram')


p <- ggplot(aes(x=user_name, y=user_followers_count), data=df)

p + geom_bar(stat="identity") + 
    scale_fill_discrete("Legend Title") + 
    labs(x="X Label", y="Y Label", title="An Example Column Chart")