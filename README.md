# Kafka-ElasticSearch-PopularUser-Twitter
A Kafka project that streams data from Twitter based on tracking terms inputted by the user. The tweet JSON objects are stored in ElasticSearch. Particular focus on "popular" users – tweets from the top 1000 most followed users are maintained, while tweets from "less" popular users are passed over. 

# Motivation 
Over the summer term of 2019 I was lucky to have a fantastic internship experience. I was originally placed in a data-related team, but due to the emergence of a large scale project I was subsequently shifted from it to another architecture-focused team. Before leaving, I had already sat through several team meetings and the frequent discussion about the use of Kafka piqued my interest. I figured that working on this project in my own time would give me some exposure to the data engineering / data streaming side of things that I would have gotten had I not switched teams - and I didn't know, maybe I'd return to my original team before my term was up! I wanted to be in a position where I could potentially help with anything. 

This combined with the fact that Twitter is a platform I frequent quite often. For the memes, for sports, for games, for politics, there's a lot that comes from Twitter that I enjoy. 

# Current Details 
Kafka 2.2-1<br/> 
Elasticsearch Version 7.3<br/> 
Make sure Kafka and Zookeeper servers are running:<br/> 
```kafka-server-start config/server.properties```<br/> 
```zookeeper-server-start config/zookeeper.properties```<br/>
- Run producer -> Run consumer 

# Next Steps / Improvements 
At the moment, this project is a purely backend-implemented project. What I have in mind for future iterations would be to create a nice dashboard where I can display all of these stored tweets, full with CSS animations (for live updating if a tweet from a more popular user is received) and just a generally aesthetic look. It would be a chance for me to learn more about web frontend and UI/UX too since I can't honestly say that I have experienced this to a sufficient level. I currently only have some basic experience with React.  
