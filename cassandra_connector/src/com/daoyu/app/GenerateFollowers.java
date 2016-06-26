package com.daoyu.app;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.io.File;

import com.lambdaworks.redis.*;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class GenerateFollowers {

  private Random ran;
  private long startid;
  private long n;
  private long memoryThreshold;
  private long count = 0;
  private Cluster cluster;
  private Session session;  
  private RedisClient redisClient;
  private RedisConnection<String, String> connection;
  private Map<Long, Set<Long>> followinglists;  
  private PrintWriter writer;
  
  @SuppressWarnings("deprecation")
  public GenerateFollowers(long startid, long n, long memoryThreshold, String cassandraDNS, String cassandraKeySpace, String redisDNS, String redisPass) {    
    // Initialize a log file
    try {
      writer = new PrintWriter("logs.txt", "UTF-8");
    } catch (FileNotFoundException | UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    
    ran = new Random();    
    this.startid = startid;    
    this.n = n;
    this.memoryThreshold = memoryThreshold;
    
    // Start conncetions for Cassandra
    cluster = Cluster.builder().addContactPoint(cassandraDNS).build();    
    session = cluster.connect(cassandraKeySpace);

    // Start conncetions for Redis
    redisClient = new RedisClient(RedisURI.create("redis://" + redisPass + "@" + redisDNS + ":6379/0"));
    connection = redisClient.connect();
    
    // Initialize cache for followinglists
    followinglists = new HashMap<Long, Set<Long>>();
    for (long i = 0; i < n; i++) {
      followinglists.put(i, new HashSet<Long>());
    }
  }
  
  public static void main(String[] args) {
    // Read from config file
    String configPath = System.getProperty("user.dir");
    String seperator = System.getProperty("file.separator");
    Config config = ConfigFactory.parseFile(new File(configPath + seperator + "conf" + seperator + "followers_generator.conf"));
    String cassandraDNS = config.getString("cassandraDNS");    
    String cassandraKeySpace = config.getString("cassandraKeySpace");
    String redisDNS = config.getString("redisDNS");
    String redisPass = config.getString("redisPass");
    long startid = config.getLong("startid");    
    long n = config.getLong("n"); 
    long memoryThreshold = config.getLong("memoryThreshold");

    GenerateFollowers genFollowingListData = new GenerateFollowers(startid, n, memoryThreshold, cassandraDNS, cassandraKeySpace, redisDNS, redisPass);    
    genFollowingListData.start();
    
    //genFollowingListData.test();
    genFollowingListData.connection.close();
    genFollowingListData.redisClient.shutdown();
    
  }
  
  private void test() {
    // Some easy queries to test connection
    String query = "SELECT followerslist FROM followerslists WHERE uid = 99999";    
    ResultSet resultSet = session.execute(query);    
    if(!resultSet.isExhausted()) {
      System.out.println(resultSet.one().getSet(0, Long.class).toString());
    }
    connection.rpush("mylist", "1230", "1234");
    List<String> value = connection.lrange("mylist", 0, -1);
    System.out.println(value);
  }
  
  private void start() {    
    System.out.println("Started.");    
    writer.println("Started.");    
    long startTime = System.currentTimeMillis();    
    for (long i = startid; i < n; i++) {      
      if (i < n*0.05/100) {
        // 0.05% users have 6% ~ 14% followers
        genDataFollowersLists(i, n*10/100, n*4/100);        
      } else if(i < n*0.15/100) {
        // 0.1% users have 3% ~ 7% followers
        genDataFollowersLists(i, n*5/100, n*2/100);        
      } else if(i < n*0.3/100) {
        // 0.15% users have 1% ~ 3% followers
        genDataFollowersLists(i, n*2/100, n*1/100);   
      } else if(i < n*0.5/100) {
        // 0.2% users have 0.5% ~ 1.5% followers
        genDataFollowersLists(i, n*1/100, n*1/200);
      } else if(i < n*10.5/100) {
        // 10% users have 100 ~ 300 followers
        genDataFollowersLists(i, 200, 100);        
      } else if(i < n*50.5/100) {
        // 40% users have 40 ~ 80 followers
        genDataFollowersLists(i, 60, 20);        
      } else if(i < n*90.5/100) {
        // 40% users have 20 ~ 40 followers
        genDataFollowersLists(i, 30, 10);        
      } else {
        // 9.5% users have 5 followers
        genDataFollowersLists(i, 5, 0);        
      }      
    }
    genDataFollowingLists(followinglists);    
    long endTime   = System.currentTimeMillis();    
    long totalTime = endTime - startTime;    
    String log = "Total time is: " + totalTime + " ms";    
    System.out.println(log);    
    writer.println(log);
    writer.close();    
  }
  
  private void genDataFollowersLists(long uid, long mean, long variance) {    
    long numOfFollowers = mean;    
    if (variance != 0) {      
      numOfFollowers += ran.nextLong() %  variance;      
    }    
    Set<Long> followers = new HashSet<Long>();    
    followers.add(uid);    
    while(followers.size() <= numOfFollowers) {      
      followers.add(Math.abs(ran.nextLong() % n));      
    }    
    followers.remove(uid);    
    String log = "Writing " + followers.size() + " followers to " + uid + ".";    
    System.out.println(log);    
    writer.println(log);
    
    String followerslistsQuery = "INSERT INTO followerslists (uid, followerslist)"        
         + " VALUES(" + uid + "," + followers.toString().replace('[', '{').replace(']', '}') + ");" ;    
    session.execute(followerslistsQuery);
    
    String key = "followers_" + uid;
    for(Long follower : followers) {
      connection.rpush(key, follower.toString());
      followinglists.get(follower).add(uid); 
    }
    count += numOfFollowers;
    // If there are more than memoryThreshold records in followinglists, flush them to database and clear memory.
    if (count > memoryThreshold) {
      genDataFollowingLists(followinglists);
    }
  }
  
  private void genDataFollowingLists(Map<Long, Set<Long>> followinglists) {
    for (Map.Entry<Long, Set<Long>> entry : followinglists.entrySet()) {      
      String log = "Writing " + entry.getValue().size() + " followings to " + entry.getKey() + ".";      
      System.out.println(log);      
      writer.println(log);        
      String followinglistsQuery = "INSERT INTO followinglists (uid, followinglist)"            
       + " VALUES(" + entry.getKey() + "," + entry.getValue().toString().replace('[', '{').replace(']', '}') + ");" ;    
      session.execute(followinglistsQuery);
      
      String key = "following_" + entry.getKey();
      for(Long follower : entry.getValue()) {
        connection.rpush(key, follower.toString());
      }
    }
    // Clear memory after flushing
    for (long i = 0; i < n; i++) {
      followinglists.get(i).clear();
    }
    count = 0;
  }  
}
