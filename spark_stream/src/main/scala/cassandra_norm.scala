import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import scala.util.parsing.json._
import java.text.DecimalFormat
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.{Date, Locale}
import java.lang.Long
import java.io.File
import com.typesafe.config.{ Config, ConfigFactory }

object TweetDataStreaming_Norm {

  val TWITTER_DATE_FORMAT:String = "EEE MMM dd HH:mm:ss Z yyyy"
  val CASSANDRA_TIMESTAMP_FORMAT:String = "yyyy-MM-dd HH:mm:ssZ"
  val twitterDateFormat = new SimpleDateFormat(TWITTER_DATE_FORMAT, Locale.ENGLISH)
  val cassandraTimestampFormat = new SimpleDateFormat(CASSANDRA_TIMESTAMP_FORMAT, Locale.ENGLISH);

  def main(args: Array[String]) {
    
    val configPath = System.getProperty("user.dir")
    val seperator = System.getProperty("file.separator")
    val config = ConfigFactory.parseFile(new File(configPath + seperator + "conf" + seperator + "spark_streaming.conf"))
    
    val duration = config.getInt("process_duration")
    val cassandra_ttl = config.getInt("cassandra_ttl")
    val n:Long = config.getLong("n")
    val spark_master:String = config.getString("spark_master")
    val topics:String = config.getString("topics")
    val appName:String = config.getString("appName")
    val cassandra_seed_public_dns:String = config.getString("cassandra_seed_public_dns")
    val key_space:String = config.getString("key_space")

    val topicsSet = topics.split(",").toSet
    val sparkConf = SparkConfSingleton.getInstance(appName, cassandra_seed_public_dns, spark_master)

    // Create context with batch interval

    val ssc = new StreamingContext(sparkConf, Seconds(duration))

    // Create direct kafka stream with brokers and topics

    val kafkaParams = Map[String, String]("metadata.broker.list" -> (spark_master + ":9092"))
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    // Get the lines and write data to cassandra

    messages.foreachRDD { rdd =>

        val lines = rdd.map(_._2)
        val tweetsStream = lines.map( rawTweet => {

                                  // Parse raw data to json

                                  val json:Option[Any] = JSON.parseFull(rawTweet)
                                  val tweet:Map[String,Any] = json.get.asInstanceOf[Map[String, Any]]
                                  val tid:Long = tweet.get("id_str").get.asInstanceOf[String].toLong
                                  val time:String = tweet.get("created_at").get.asInstanceOf[String]
                                  val created_at:String = cassandraTimestampFormat.format(twitterDateFormat.parse(time))
                                  val user:Map[String,Any] = tweet.get("user").get.asInstanceOf[Map[String, Any]]
                                  val uid:Long = user.get("id_str").get.asInstanceOf[String].toLong % n

                                  // Query for followers

                                  import com.datastax.spark.connector.cql.CassandraConnector
                                  import com.datastax.driver.core.ResultSet
                                  import com.datastax.driver.core.Row
                                  import collection.JavaConverters._
                                  
                                  val conf = SparkConfSingleton.getInstance(appName, cassandra_seed_public_dns, spark_master)
                                  var resultSet:ResultSet = null
                                  CassandraConnector(conf).withSessionDo { session =>
                                       resultSet = session.execute( s"SELECT followerslist FROM rettiwt.followerslists WHERE uid = $uid")
                                  }
                                  var followerslist:Set[Long] = null

                                  // User 0 will listen to all tweets

                                  val listener:Long = 0
                                  if (!resultSet.isExhausted) {
                                    val followers = resultSet.one.getSet("followerslist", classOf[java.lang.Long])
                                    followers.add(listener)
                                    followerslist = followers.asScala.toSet
                                  }
                                  (tid, uid, followerslist, created_at, rawTweet)
        })

        // Save data
        
        import com.datastax.spark.connector.writer._
        tweetsStream.map(x => (x._1, x._5)).saveToCassandra(key_space, "tweets", SomeColumns("tid", "tweet"), writeConf = WriteConf(ttl = TTLOption.constant(cassandra_ttl)))
        val tweetsStreamReduced = tweetsStream.map(x => (x._1, x._2, x._3, x._4))
        tweetsStreamReduced.map(x => (x._2, x._4,x._1)).saveToCassandra(key_space, "homepages", SomeColumns("uid", "time", "tid"), writeConf = WriteConf(ttl = TTLOption.constant(cassandra_ttl)))

        tweetsStream.filter(_._3 != null).map(tweet => {
                                              tweet._3.map( follower => {
                                                (follower, tweet._4, tweet._1)
                                              })
        }).flatMap(x => x.map(y => y)).saveToCassandra(key_space, "inboxes", SomeColumns("uid", "time", "tid"), writeConf = WriteConf(ttl = TTLOption.constant(cassandra_ttl)))
    }
    ssc.start()
    ssc.awaitTermination()
  }
}