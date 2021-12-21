package apache.zap.spark.core.rdds

/**
  * Syntax : def fold(zeroValue: T)(op: (T, T) => T)
  *
  * zeroValue the initial value for the accumulated result of each partition for the op operator, and
  * also the initial value for the combine results from different partitions for the op operator.
  *
  * Aggregate the elements of each partition, and then the results for all the partitions, using a
  * given associative function and a neutral "zero value".
  *
  * It takes function as an input which has two parameters of the same type and outputs a single value
  * of the input type.
  *
  * Points to Note:
  * 1. fold() is similar to reduce() except it takes a ‘Zero value‘ as an initial value for each partition.
  * 2. fold() is similar to aggregate() with a difference; fold return type should be the same as this RDD element
  * type whereas aggregation can return any type.
  * 3. fold() also same as foldByKey() except foldByKey() operates on Pair RDD
  *
  */
object FoldRdd extends App{

  val rdd1 = sc.parallelize(List(1,2,3,4,5,6,7,8,9),3) //sc.parallelize(value,noOfPartitions)

  println(rdd1.partitions.length) // to know the number of partitions for particular RDD. => 3

  //rdd1 data will get distribute across all 3 partitions
  // scala> rdd1.glom.collect
  // res1: Array[Array[Int]] = Array(Array(1, 2, 3), Array(4, 5, 6), Array(7, 8, 9)).
  // It will get calculate as below.
  // In partition1 => (1+2+3)+2 = 8
  // In partition2 => (4+5+6)+2 = 17
  // In partition3 => (7+8+9)+2 = 26
  // Total across partition = 8+17+26 = 51
  // Combining the result = 51 + 2 = 53
  val sum1 = rdd1.fold(2)((acc, value) => acc + value )
  val sum2 = rdd1.fold(2)(_+_)

  println(sum1)
  println(sum2)

  //Calculate Next highest salary of the employee in company
  val employee = sc.parallelize(List(
    ("Rob",50000),
    ("Edward", 70000),
    ("Michal", 45000),
    ("George",65000),
    ("Steve",40000)
  ),3)

  val refEmployee = ("RefEmployee",60000)

  val nextHighestSalary = employee.fold(refEmployee)((acc, value) => {
    if(value._2 > acc._2) value else acc
  })

  println("Next highest Salary = " + nextHighestSalary)

  //Calculate largest number
  val numberList = sc.parallelize(List(22,45,78,50,56,99,73,11))

  val largestNumber = numberList.fold(0)((acc,value) => {
    Math.max(acc,value)
  })

  println("Largest number = " + largestNumber)
}
