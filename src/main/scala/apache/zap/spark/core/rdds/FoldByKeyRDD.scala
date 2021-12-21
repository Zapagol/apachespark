package apache.zap.spark.core.rdds


/**
  * 1. def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
  *
  * 2. def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDD[(K, V)]
  *
  * Merge the values for each key using an associative function and a neutral "zero value" which
  * may be added to the result an arbitrary number of times, and must not change the result
  * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
  *
  * foldByKey should be used in use cases where values need to be aggregated based on keys's.
  *
  */

object FoldByKeyRDD extends App {

  val employeeData = sc.parallelize(List(
    ("BigData Engineer",("Jack",95000,7)),
    ("BigData Engineer",("Edward",125000,8)),
    ("BigData Engineer",("Edward",60000,4)),
    ("Java Developer",("Root",55000,5)),
    ("FullStack Developer",("John",65000,5)),
    ("FullStack Developer",("Rock",100000,7)),
    ("Java Developer",("Tom",80000,8)))
  )

  val maxSalByProfile1 = employeeData.foldByKey(("dummy",0,0)) ((acc,element) => {
    if(acc._2 > element._2) acc else element
  })

  println("Maximum salaries in each profile = " + maxSalByProfile1.collect().toList)

  //Maximum salaries in each profile = List((FullStack Developer,(Rock,100000,7)), (BigData Engineer,(Edward,125000,8)),
  // (Java Developer,(Tom,80000,8)))


}
