/**
  * Created by rohith on 27/4/17.
  */

import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object InteractiveQuerying {
  val conf = new SparkConf().setMaster("local[*]").setAppName("irdd sorting")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val sqlContext = new SQLContext(sc)
  val ssc = new StreamingContext(sc, Seconds(10))

  val stream1 = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY)
  val wordStream = stream1.flatMap(x => x.split(" "))

  val wordWindowStream = wordStream.window(Seconds(30))

  def findTopWords(): Unit = {
    val result = sqlContext.sql("SELECT * FROM word_count")
    result.rdd.foreach(println)
  }

  wordWindowStream.foreachRDD{ rdd =>
    val frequency = rdd.map(x => (x, 1.0)).reduceByKey(_+_)
    val rddToDf = sqlContext.createDataFrame(frequency).toDF("word", "count").registerTempTable("word_count")
    frequency.take(1)
    findTopWords()
  }

  ssc.start()
  ssc.awaitTermination()
}
