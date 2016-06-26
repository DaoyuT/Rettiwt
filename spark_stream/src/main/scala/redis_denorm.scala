import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import scala.util.parsing.json._
import java.text.DecimalFormat
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.{Date, Locale}
import java.lang.Long
import collection.JavaConverters._
import com.lambdaworks.redis._
import java.io.File
import com.typesafe.config.{ Config, ConfigFactory }

object TweetDataStreaming_Redis_Denorm {

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
    val redis_pass:String = config.getString("redis_pass")
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
                                  
                                  val connection = SerializedRedisConnection.getInstance(redis_pass, spark_master)
                                  val followerslist:List[String] = connection.lrange("followers_" + uid, 0, -1).asScala.toList
                                  (tid, uid, followerslist, created_at, rawTweet)
        })

        // Save data

        import com.datastax.spark.connector.writer._
        tweetsStream.map(x => (x._2, x._4,x._5)).saveToCassandra(key_space, "historytweets", SomeColumns("uid", "time", "tweet"), writeConf = WriteConf(ttl = TTLOption.constant(cassandra_ttl)))

        tweetsStream.filter(_._3 != null).map(tweet => {
                                              tweet._3.map( follower => {
                                                (follower.toLong, tweet._4, tweet._5)
                                              })
        }).flatMap(x => x.map(y => y)).saveToCassandra(key_space, "feedslists", SomeColumns("uid", "time", "tweet"), writeConf = WriteConf(ttl = TTLOption.constant(cassandra_ttl)))
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
