import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * Created by rohith on 31/1/17.
  */

object CustomPartitionerTest {
  class CustomPartitioner(num: Int) extends Partitioner {

    // define your custom partition classification method
    def getPartitionNumber(max: Int, key: Any): Int = {
      Math.abs(key.hashCode()) % max
    }

    override def numPartitions: Int = num

    override def getPartition(key: Any): Int = {
      val partitionNum = {getPartitionNumber(num, key)}
      println(s"current key : $key belongs to $partitionNum")
      partitionNum
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test partition")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 100).repartition(10).keyBy(_ * 2).partitionBy(new CustomPartitioner(20))
    rdd.saveAsTextFile("data/int")
  }
}

