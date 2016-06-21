package com.daoyu.app;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.*;

import com.lambdaworks.redis.*;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class GenFollowersListData {

  private Random ran;
  private long startid;
  private long n;
  private long count = 0;
  private Cluster cluster;
  private Session session;  
  private RedisClient redisClient;
  private RedisConnection<String, String> connection;
  private Map<Long, Set<Long>> followinglists;  
  private PrintWriter writer;
  
  public GenFollowersListData(long startid, long n, String cassandraDNS, String cassandraKeySpace, String redisDNS, String redisPass) {    
    try {
      writer = new PrintWriter("logs.txt", "UTF-8");
    } catch (FileNotFoundException | UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    ran = new Random();    
    this.startid = startid;    
    this.n = n;    
    cluster = Cluster.builder().addContactPoint(cassandraDNS).build();    
    session = cluster.connect(cassandraKeySpace);
    redisClient = new RedisClient(RedisURI.create("redis://" + redisPass + "@" + redisDNS + ":6379/0"));
    connection = redisClient.connect();
    for (long i = 0; i < n; i++) {
      followinglists.put(i, new HashSet<Long>());
    }
  }
  
  public static void main(String[] args) {
    String cassandraDNS = "ec2-52-205-12-100.compute-1.amazonaws.com";    
    String cassandraKeySpace = "rettiwt";
    String redisDNS = "ec2-52-6-127-71.compute-1.amazonaws.com";
    String redisPass = "tudaoyu";
    long startid = Long.parseLong(Integer.toString(0));    
    long n = Long.parseLong(Integer.toString(100000));    
    GenFollowersListData genFollowingListData = new GenFollowersListData(startid, n, cassandraDNS, cassandraKeySpace, redisDNS, redisPass);    
    //genFollowingListData.start();    
    //genFollowingListData.test();
  }
  
  private void test() {
    String query = "SELECT followerslist FROM followerslists WHERE uid = 99999";    
    ResultSet resultSet = session.execute(query);    
    if(!resultSet.isExhausted()) {
      System.out.println(resultSet.one().getSet(0, Long.class).toString());
    }
  }
  
  private void start() {    
    System.out.println("Started.");    
    writer.println("Started.");    
    long startTime = System.currentTimeMillis();    
    for (long i = startid; i < n; i++) {      
      if (i < n*1/100) {        
        genDataFollowersLists(i, n*20/100, n*4/100);        
      } else if(i < n*5/100) {        
        genDataFollowersLists(i, n*10/100, n*2/100);        
      } else if(i < n*10/100) {        
        genDataFollowersLists(i, n*5/100, n*1/100);        
      } else if(i < n*30/100) {        
        genDataFollowersLists(i, 400, 200);        
      } else if(i < n*70/100) {        
        genDataFollowersLists(i, 150, 50);        
      } else if(i < n*90/100) {        
        genDataFollowersLists(i, 75, 25);        
      } else {        
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
    for (long follower: followers) {      
      followinglists.get(follower).add(uid);      
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
    }
  }  
}
