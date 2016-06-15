CREATE KEYSPACE rettiwt WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' :3};

USE rettiwt;

CREATE TABLE feedslists (
	uid bigint, 
	time timestamp, 
	tweet text, 
	PRIMARY KEY (uid, time) 
) WITH CLUSTERING ORDER BY (time DESC);

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
