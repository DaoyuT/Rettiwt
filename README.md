#Rettiwt- Rebuilding the Twitter Platform
This is the project I carried out during the seven-week [Insight Data Engineering Fellows Program](http://http://www.insightdataengineering.com/) which is designed for people with strong knowledge of computer science fundamentals and coding skills to transition to data engineering by giving them a space to get hands-on experience building out distributed platforms on AWS using open source technologies.</br>
Rettiwt is a twitter-like social media platform that delivers real-time streaming data from twitter api to all the simulated users. Chech it out at [daoyu.online](http://www.daoyu.online/) and here is another [video demo](https://youtu.be/mQ0-5NMxCAc).

##Data Pipeline
![alt tag](https://raw.githubusercontent.com/dytu0316/Rettiwt/master/data_pipeline.png)

##Code Structure
* ***cassandra_connector*** </br>
A side project written in JAVA to simulate users' names and followers and to compare reading rate from normalized schema and denormalized schema.
* ***cassandra_schema*** </br>
CQL language of schema in Cassandra database.
* ***flask*** </br>
A frontend implementation using flask(This framework is sooo convenient!!!) written in python. Bootstrap was applied for the web design.
* ***kafka*** </br>
A kafka producer written in python. Used tweepy to call twitter api to get real-time tweets and pipe them into kafka.
* ***spark_stream*** </br>
A Spark streaming process written in Scala which delivers the tweets. Used spark-cassandra-connector and lettuce to connect Cassandra and Redis.
