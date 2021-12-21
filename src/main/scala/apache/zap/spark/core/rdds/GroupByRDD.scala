package apache.zap.spark.core.rdds


/**
  * def groupBy[K: ClassTag](f: T => K): RDD[(K, Iterable[T])]
  * def groupBy[K: ClassTag](f: T => K, numPartitions: Int): RDD[(K, Iterable[T])]
  * def groupBy[K: ClassTag](f: T => K, p: Partitioner): RDD[(K, Iterable[T])]
  *
  * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
  * mapping to that key. The ordering of elements within each group is not guaranteed, and
  * may even differ each time the resulting RDD is evaluated.
  *
  * groupBy() can be used in both unpaired & paired RDDs. When used with unpaired data, the key
  * for groupBy() is decided by the function literal passed to the method.
  */
object GroupByRDD extends App {

  //Example 1 : Group the list of age's into "child", "adult" and "senior"
  val ageRdd = sc.parallelize(List(6, 29, 54, 16, 75, 50, 60, 85,
    11, 29, 30, 82, 64, 38, 17, 30, 10))
  val groupedAge = ageRdd.groupBy(age => {
    if(age >=18 && age <= 60) "adult"
    else if (age < 18) "child"
    else "senior"
  })

  println("Grouped Age --->")
  groupedAge.collect().foreach(println)

//  (senior,CompactBuffer(75, 85, 82, 64))
//  (child,CompactBuffer(6, 16, 11, 17, 10))
//  (adult,CompactBuffer(29, 54, 50, 60, 29, 30, 38, 30))

  //Example 2 : Group the Even and Odd number
  val numbersRdd = sc.parallelize(List(6, 29, 54, 16, 75, 50, 60, 85,
    11, 29, 30, 82, 64, 38, 17, 30, 10))

  val groupedNumbers = numbersRdd.groupBy(number => {
    number % 2 match {
      case 0 => "EvenNumebrs"
      case _ => "OddNumbers"
    }
  })

  println("Grouped Numbers --->")
  groupedNumbers.collect.foreach(println)

//  (OddNumbers,CompactBuffer(29, 75, 85, 11, 29, 17))
//  (EvenNumebrs,CompactBuffer(6, 54, 16, 50, 60, 30, 82, 64, 38, 30, 10))

}
