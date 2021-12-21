package apache.zap.spark.core.rdds

/**
  * Generic function to combine the elements for each key using a custom set of aggregation
  * functions. Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a "combined type" C
  *
  * Users provide three functions:
  *
  *  - `createCombiner`, which turns a V into a C (e.g., creates a one-element list). is called when a key(in the
  *  RDD element) is found for the first time in a given Partition. This method creates an initial value for the
  *  accumulator for that key
  *  - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list). This is called when the key
  *  already has an accumulator
  *  - `mergeCombiners`, to combine two C's into a single one. is called when more that one partition has
  *  accumulator for the same key
  *
  */
object CombineByKey extends App {

  val tuple = sc.parallelize(Seq(
    ("k1", 12),("k2", 13),("k1", 11),("k1", 16),("k2", 14),("k3", 11),("k2", 14),("k1", 14),("k3", 12)
  ))

  tuple.combineByKey(
    v => (v, 1), //createCombiner
    (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), // mergeValue
    (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)) // mergeCombiner
//    .map{
//      case (key, value) => (key, value._1 / value._2)
//    }
    .collect()
    .foreach(println)

//  (k3,(23,2))
//  (k2,(41,3))
//  (k1,(53,4))

}
