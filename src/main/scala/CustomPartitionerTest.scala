import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * Created by rohith on 31/1/17.
  */

object CustomPartitionerTest {
  class CustomPartitioner(allKeys: List[Any]) extends Partitioner {
    val allKeysPartitions = allKeys.zipWithIndex.toMap
    // define your custom partition classification method
    def getPartitionNumber(key: Any): Int = {
      allKeysPartitions.get(key) match{
        case Some(parNum) => parNum
        case None => 0
      }
    }

    override def numPartitions: Int = allKeys.length

    override def getPartition(key: Any): Int = {
      val partitionNum = getPartitionNumber(key)
      println(s"current key : $key belongs to $partitionNum")
      partitionNum
    }
  }

  def partitionExample(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test partition")
    val sc = new SparkContext(conf)
    val allKeys = 1 to 20
    val rdd = sc.parallelize(1 to 20).repartition(10).keyBy(_ * 2).partitionBy(new CustomPartitioner(allKeys.toList))
    rdd.saveAsTextFile("data/ParInfo")
  }
}

