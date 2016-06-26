CREATE KEYSPACE rettiwt WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' :3};

USE rettiwt;

CREATE TABLE followinglists (
    uid bigint,
    followinglist set<bigint> ,
    PRIMARY KEY (uid)
);

CREATE TABLE followerslists (
    uid bigint,
    followerslist set<bigint> ,
    PRIMARY KEY (uid)
);

# Denormalized schema

CREATE TABLE feedslists (
	uid bigint, 
	time timestamp, 
	tweet text, 
	PRIMARY KEY (uid, time) 
) WITH CLUSTERING ORDER BY (time DESC);

CREATE TABLE historytweets (
	uid bigint, 
	time timestamp, 
	tweet text, 
	PRIMARY KEY (uid, time) 
) WITH CLUSTERING ORDER BY (time DESC);

# Normalized schema

CREATE TABLE inboxes (
	uid bigint, 
	time timestamp, 
	tid bigint, 
	PRIMARY KEY (uid, time) 
) WITH CLUSTERING ORDER BY (time DESC);

CREATE TABLE homepages (
	uid bigint, 
	time timestamp, 
	tid bigint, 
	PRIMARY KEY (uid, time) 
) WITH CLUSTERING ORDER BY (time DESC);

CREATE TABLE tweets (
	tid bigint, 
	tweet text, 
	PRIMARY KEY (tid) 
);


