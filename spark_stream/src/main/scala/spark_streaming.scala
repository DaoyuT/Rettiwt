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

object TweetDataStreaming {

  val TWITTER_DATE_FORMAT:String = "EEE MMM dd HH:mm:ss Z yyyy"
  val CASSANDRA_TIMESTAMP_FORMAT:String = "yyyy-MM-dd HH:mm:ssZ"
  val twitterDateFormat = new SimpleDateFormat(TWITTER_DATE_FORMAT, Locale.ENGLISH)
  val cassandraTimestampFormat = new SimpleDateFormat(CASSANDRA_TIMESTAMP_FORMAT, Locale.ENGLISH);

  def main(args: Array[String]) {

    val duration = 2
    val cassandra_ttl = 86400
    val n:Long = 100000
    val spark_master:String = "ec2-52-6-127-71.compute-1.amazonaws.com"
    val topics:String = "tweet_stream"
    val appName:String = "tweet_data"
    val cassandra_seed_public_dns:String = "ec2-52-205-12-100.compute-1.amazonaws.com"
    val key_space:String = "rettiwt"

    val topicsSet = topics.split(",").toSet
    val sparkConf = new SparkConf().setAppName(appName).set("spark.cassandra.connection.host", cassandra_seed_public_dns).setMaster("spark://" + spark_master + ":7077")

    // Create context with 2 second batch interval

    val ssc = new StreamingContext(sparkConf, Seconds(duration))

    // Create direct kafka stream with brokers and topics

    val kafkaParams = Map[String, String]("metadata.broker.list" -> (spark_master + ":9092"))
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    // Get the lines and write data to cassandra

    messages.foreachRDD { rdd =>

        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
        import sqlContext.implicits._
        val lines = rdd.map(_._2)
        val tweetsStream = lines.map( rawTweet => {

                                  val json:Option[Any] = JSON.parseFull(rawTweet)
                                  val tweet:Map[String,Any] = json.get.asInstanceOf[Map[String, Any]]
                                  val tid:Long = tweet.get("id_str").get.asInstanceOf[String].toLong
                                  val time:String = tweet.get("created_at").get.asInstanceOf[String]
                                  val created_at:String = cassandraTimestampFormat.format(twitterDateFormat.parse(time))
                                  val user:Map[String,Any] = tweet.get("user").get.asInstanceOf[Map[String, Any]]
                                  val uid:Long = user.get("id_str").get.asInstanceOf[String].toLong % n

                                  import com.datastax.spark.connector.cql.CassandraConnector
                                  import com.datastax.driver.core.ResultSet
                                  import com.datastax.driver.core.Row
                                  import collection.JavaConverters._
                                  
                                  val conf = new SparkConf().setAppName(appName).set("spark.cassandra.connection.host", cassandra_seed_public_dns).setMaster("spark://" + spark_master + ":7077")
                                  var resultSet:ResultSet = null
                                  CassandraConnector(conf).withSessionDo { session =>
                                       resultSet = session.execute( s"SELECT followerslist FROM rettiwt.followerslists WHERE uid = $uid")
                                  }
                                  var followerslist:Set[Long] = null
                                  if (!resultSet.isExhausted) {
                                    followerslist = resultSet.one.getSet("followerslist", classOf[java.lang.Long]).asScala.toSet
                                  }
                                  (tid, uid, followerslist, created_at, rawTweet)
        })

        import com.datastax.spark.connector.writer._
        tweetsStream.map(x => (x._2, x._4,x._5)).saveToCassandra(key_space, "historytweets", SomeColumns("uid", "time", "tweet"), writeConf = WriteConf(ttl = TTLOption.constant(cassandra_ttl)))

        tweetsStream.filter(_._3 != null).map(tweet => {
                                              tweet._3.map( follower => {
                                                (follower, tweet._4, tweet._5)
                                              })
        }).flatMap(x => x.map(y => y)).saveToCassandra(key_space, "feedslists", SomeColumns("uid", "time", "tweet"), writeConf = WriteConf(ttl = TTLOption.constant(cassandra_ttl)))
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

/** Instantiated singleton instance of SQLContext */

object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {

    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }

    instance
  }
}
