package apache.zap.spark.core.rdds

/**
  * 1. mapValues : def mapValues[U](f: (V) ⇒ U): RDD[(K, U)]
  *       Pass each value in the key-value pair RDD through a map function without changing the keys;
  *       this also retains the original RDD's partitioning.mapValues operates on the value only (the
  *       second part of the tuple)
  *       In other words, given f: B => C and rdd: RDD[(A, B)], these two are identical.
  *
  *       val result: RDD[(A, C)] = rdd.map { case (k, v) => (k, f(v)) }
  *       val result: RDD[(A, C)] = rdd.mapValues(f)
  *
  * 2. map : def map[U: ClassTag](f: T => U): RDD[U]
  *       Return a new RDD by applying a function to all elements of this RDD.map is applicable for
  *       both RDD and PairRDD.map operates on the entire record(tuple of key and value).
  *       When we use map() with a Pair RDD, we get access to both Key & value.
  *       map takes a function that transforma each element of a collection.
  *
  */
object mapAndmapValuesRDD extends App {

  //Example 1 :
  val numbRdd = sc.parallelize(List(
    (1,2),
    (3,4),
    (3,6)
  ))

  //mapValues would only pass the values to your function.
  val mvRdd = numbRdd.mapValues(x => x+1)
  mvRdd.collect.foreach(println)

  //When we use map() with a Pair RDD, we get access to both Key & value.
  //map would pass the entire record(tuple of key and value)
  val mapRdd = numbRdd.map(x => (x._1+1, x._2+1))
  mapRdd.collect.foreach(println)

  //Example 2 :
  val noOfworldCups = sc.parallelize(Seq(("India", 2), ("England", 0), ("Australia", 5)))

  //map operates on the entire record.
  val mapRdd2 = noOfworldCups.map(x => (x,1))
  mapRdd2.collect.foreach(println) // ((India,2),1), ((England,0),1), ((Australia,5),1)

  //mapValues operates on the value only.
  val mapValuesRdd2 = noOfworldCups.mapValues(x => (x,1))
  mapValuesRdd2.collect.foreach(println) // (India,(2,1)), (England,(0,1)), (Australia,(5,1))


}
