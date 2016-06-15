import kafka.serializer.StringDecoder

import org.apache.spark.streaming._

import org.apache.spark.streaming.kafka._

import org.apache.spark.SparkConf

import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext

import org.apache.spark.sql._

import com.datastax.spark.connector._

import scala.util.parsing.json._

object TweetDataStreaming {

  def main(args: Array[String]) {

    val brokers = "ec2-52-22-61-135.compute-1.amazonaws.com:9092"

    val topics = "tweet_stream"

    val topicsSet = topics.split(",").toSet

    // Create context with 2 second batch interval

    val sparkConf = new SparkConf().setAppName("tweet_data").set("spark.cassandra.connection.host", "ec2-52-22-61-135.compute-1.amazonaws.com")

    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // Create direct kafka stream with brokers and topics

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    // Get the lines and show results

    messages.foreachRDD { rdd =>

        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)

        import sqlContext.implicits._

        val lines = rdd.map(_._2)

        val tweetsDF = lines.map( x => {

                                  val json:Option[Any] = JSON.parseFull(x)

                                  val map:Map[String,Any] = json.get.asInstanceOf[Map[String, Any]]

                                  val id:Long = map.get("id_str").get.asInstanceOf[String].toLong

                                  (id,x)})

        tweetsDF.saveToCassandra("rettiwt", "simpletable", SomeColumns("id", "tweet"))

        tweetsDF.toDF().show()

    }

    // Start the computation

    ssc.start()

    ssc.awaitTermination()

  }

}

/** Lazily instantiated singleton instance of SQLContext */

object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {

    if (instance == null) {

      instance = new SQLContext(sparkContext)

    }

    instance

  }

}
