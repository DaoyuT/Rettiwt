package com.daoyu.app;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.io.File;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class Test2Schema {

  private Random ran;
  private long n;
  private long t;
  private Cluster cluster;
  private Session session; 
  private PrintWriter writer;
  
  @SuppressWarnings("deprecation")
  public Test2Schema(long n, long t, String cassandraDNS, String cassandraKeySpace) {    
    // Initialize a log file
    try {
      writer = new PrintWriter("logs.txt", "UTF-8");
    } catch (FileNotFoundException | UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    
    ran = new Random();
    this.n = n;
    this.t = t;
    
    // Start conncetions for Cassandra
    cluster = Cluster.builder().addContactPoint(cassandraDNS).build();    
    session = cluster.connect(cassandraKeySpace);
  }
  
  public static void main(String[] args) {
    // Read from config file
    String configPath = System.getProperty("user.dir");
    String seperator = System.getProperty("file.separator");
    Config config = ConfigFactory.parseFile(new File(configPath + seperator + "conf" + seperator + "followers_generator.conf"));
    String cassandraDNS = config.getString("cassandraDNS");    
    String cassandraKeySpace = config.getString("cassandraKeySpace");
    long n = config.getLong("n");
    long t = config.getLong("numOfTests");

    Test2Schema test2Schema = new Test2Schema(n, t, cassandraDNS, cassandraKeySpace);    
    test2Schema.start();    
  }
  
  private void start() {
    String log = "Started.";
    System.out.println(log);    
    writer.println(log);
    long startTime, endTime, totalNorm = 0, totalDenorm = 0;
    for (long i = 0; i < t; i++) {
      long uid = ran.nextLong() % n;
      
      // Phase 1, run tests for normalized schema...
      startTime = System.currentTimeMillis();
      String inboxesQuery = "SELECT tid FROM inboxes WHERE uid = " + uid + " LIMIT 20;";
      ResultSet resultSet = session.execute(inboxesQuery);
      Set<Long> tidSet = new HashSet<Long>();
      for (Row row : resultSet) {
        tidSet.add(row.getLong(0));
      }
      String tweetsQuery = "SELECT tweet FROM tweets WHERE tid in " + tidSet.toString().replace('[', '(').replace(']', ')') + ";";
      session.execute(tweetsQuery);
      endTime = System.currentTimeMillis();
      totalNorm += endTime - startTime;
      
      //Phase 2, run tests for denormalized schema...
      String feedslistsQuery = "SELECT tweet FROM feedslists WHERE uid = " + uid + " LIMIT 20;";
      resultSet = session.execute(feedslistsQuery);
      totalDenorm += System.currentTimeMillis() - endTime;
    }
    log = "Test of retriving 20 records for " + t
    + " times.\nTotal time for Normalized Schema is: " + totalNorm + " ms";
    System.out.println(log);
    writer.println(log);
    log = "Total time for Denormalized Schema is: " + totalDenorm + " ms";
    System.out.println(log);
    writer.println(log);
    writer.close();
  }
}