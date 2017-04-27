import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by rohith on 27/4/17.
  */

object RDDtoDStreamExample extends App {
  val conf = new SparkConf().setMaster("local[*]").setAppName("irdd sorting")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val sqlContext = new SQLContext(sc)
  val ssc = new StreamingContext(sc, Seconds(10))

  val stream1 = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY)


  val rdd = sc.parallelize(1 to 10).map(_.toString)
  val queue = new scala.collection.mutable.Queue[RDD[String]]
  queue.enqueue(rdd)

  val stream2 = ssc.queueStream(queue, false)

  val bigstream = stream1.union(stream2)
  val windowStream = bigstream.window(Seconds(60))

  windowStream.foreachRDD{ rdd =>
    println(s"${rdd.collect().toSeq}")
  }

  ssc.start()
  ssc.awaitTermination()
}
