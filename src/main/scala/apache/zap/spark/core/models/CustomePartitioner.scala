package apache.zap.spark.core.models

import org.apache.spark._

/**
  * numPartitions: Int, it takes the number of partitions that needs to be created.
  *
  * getPartition(key: Any): Int, this method will return the particular key to the specified
  * partition ID which ranges from 0 to numPartitions-1 for a given key.
  *
  * Equals(): is the normal java equality method used to compare two objects, this method
  * will test your partitioner object against other objects of itself then it decides whether two
  * of your RDDs are Partitioned in the same way or not.
  *
  * Syntax: rdd.partitionBy(new CustomPartitioner(numberOfPartitioner))
  *
  * Depending on the requirement need to modify custom partitioner and the number of partitions.
  *
  * @param numberOfPartitioner
  */
class CustomePartitioner(numberOfPartitioner: Int) extends Partitioner {

  override def numPartitions: Int = numberOfPartitioner
  override def getPartition(key: Any): Int = key match {
    case "Zomato"     => 0
    case "BookMyShow" => 1
    case "Swiggy"     => 2
  }

//  override def equals(other: Any): Boolean = other match {
//    case partitioner: CustomePartitioner =>
//      partitioner.numPartitions == numPartitions
//    case _ =>
//      false
//  }

}
