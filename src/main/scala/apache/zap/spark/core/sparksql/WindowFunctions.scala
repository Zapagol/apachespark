package apache.zap.spark.core.sparksql

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Window functions operate on a group of rows, referred to as a window, and
  * calculate a return value for each row based on the group of rows.
  *
  * Window functions are useful for processing tasks such as calculating a moving average,
  * computing a cumulative statistic, or accessing the value of rows given the relative
  * position of the current row.
  *
  * Syntax:
  * window_function OVER
  * ( [  { PARTITION | DISTRIBUTE } BY partition_col_name = partition_col_val ( [ , ... ] ) ]
  * { ORDER | SORT } BY expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [ , ... ]
  * [ window_frame ] )
  */
object WindowFunctions extends App {

  import spark.implicits._
  case class Salary(dept: String, id: Long, salary: Long)
  val employeeDS = Seq(
    Salary("sales", 1, 5000),
    Salary("personnel", 2, 3900),
    Salary("sales", 3, 4800),
    Salary("sales", 4, 4800),
    Salary("personnel", 5, 3500),
    Salary("develop", 7, 4200),
    Salary("develop", 8, 6000),
    Salary("develop", 9, 4500),
    Salary("develop", 10, 5200),
    Salary("develop", 11, 5200),
    Salary("develop", 11, 5200)
  ).toDS()

  val rankDF10 = employeeDS
    .withColumn("rank", rank() over Window.partitionBy("dept", "id").orderBy("salary"))
  rankDF10.show(false)

  /**
    * Window Aggregate Functions
    */
  //Define specification of window. Get aggregated data based on dept
  val byDeptName = Window.partitionBy("dept")

  //Apply aggregate function on Window
  val agg_sal = employeeDS
    .withColumn("max_salary", max("salary").over(byDeptName))
    .withColumn("min_salary", min("salary").over(byDeptName))
    .select("dept", "max_salary", "min_salary")
    .dropDuplicates()

  agg_sal.show(false)

  //  +---------+----------+----------+
  //  |depName  |max_salary|min_salary|
  //  +---------+----------+----------+
  //  |develop  |6000      |4200      |
  //  |sales    |5000      |4800      |
  //  |personnel|3900      |3500      |
  //  +---------+----------+----------+

  //using sql
  employeeDS.createOrReplaceTempView("employee")
  spark
    .sql(
      "select dept, MIN(salary) over (partition by dept) as min_salary, MAX(salary) over (partition by dept) as max_salary from employee")
    .dropDuplicates()
    .show(false)

  /**
    * Window Ranking Functions
     */

  //  +---------+----------+----------+
  //  |depName  |max_salary|min_salary|
  //  +---------+----------+----------+
  //  |develop  |6000      |4200      |
  //  |sales    |5000      |4800      |
  //  |personnel|3900      |3500      |
  //  +---------+----------+----------+

  //Using dataframe
  //Create window specification for ranking functions
  val windowSpecification1 = Window.partitionBy("dept").orderBy("salary")

  //1. Rank function: rank() : This function will return the rank of each record and skip the subsequent rank following any duplicate rank
  val rankDF1 = employeeDS
    .withColumn("rank", rank().over(windowSpecification1))
  rankDF1.show(false)
  val rankDF2 = employeeDS
    .withColumn("rank", rank() over Window.partitionBy("dept").orderBy("salary"))

  val rankDF3 = employeeDS
    .withColumn("rank", rank() over Window.partitionBy("dept", "id", "salary").orderBy("salary"))
  rankDF3.show(false)
  rankDF2.show(false)

  //  +---------+---+------+----+
  //  |dept     |id |salary|rank|
  //  +---------+---+------+----+
  //  |develop  |7  |4200  |1   |
  //  |develop  |9  |4500  |2   |
  //  |develop  |10 |5200  |3   |
  //  |develop  |11 |5200  |3   |
  //  |develop  |8  |6000  |5   |
  //  |sales    |3  |4800  |1   |
  //  |sales    |4  |4800  |1   |
  //  |sales    |1  |5000  |3   |
  //  |personnel|5  |3500  |1   |
  //  |personnel|2  |3900  |2   |
  //  +---------+---+------+----+

    //using sql
  spark
    .sql("select dept, id, salary, rank() over (PARTITION BY dept ORDER BY salary) as rank from employee")
    .show(false)

  //  +---------+---+------+----+
  //  |dept     |id |salary|rank|
  //  +---------+---+------+----+
  //  |personnel|5  |3500  |1   |
  //  |personnel|2  |3900  |2   |
  //  |develop  |7  |4200  |3   |
  //  |develop  |9  |4500  |4   |
  //  |sales    |3  |4800  |5   |
  //  |sales    |4  |4800  |5   |
  //  |sales    |1  |5000  |7   |
  //  |develop  |10 |5200  |8   |
  //  |develop  |11 |5200  |8   |
  //  |develop  |8  |6000  |10  |
  //  +---------+---+------+----+

  // 2. Dense Rank(dense_rank()): This function will return the rank of each record within a partition but will not skip any rank.
  val denseRankDF1 = employeeDS
    .withColumn("dense_rank", dense_rank().over(windowSpecification1))
  denseRankDF1.show(false)
  val denseRankDF2 = employeeDS
    .withColumn("dense_rank", dense_rank() over Window.partitionBy("dept").orderBy("salary"))
  denseRankDF2.show(false)

  //  +---------+---+------+----------+
  //  |dept     |id |salary|dense_rank|
  //  +---------+---+------+----------+
  //  |develop  |7  |4200  |1         |
  //  |develop  |9  |4500  |2         |
  //  |develop  |10 |5200  |3         |
  //  |develop  |11 |5200  |3         |
  //  |develop  |8  |6000  |4         |
  //  |sales    |3  |4800  |1         |
  //  |sales    |4  |4800  |1         |
  //  |sales    |1  |5000  |2         |
  //  |personnel|5  |3500  |1         |
  //  |personnel|2  |3900  |2         |
  //  +---------+---+------+----------+

  //using sql
  spark
    .sql("select dept, id, salary, dense_rank() over (PARTITION BY dept ORDER BY salary) as dense_rank from employee")
    .show(false)
  //  +---------+---+------+----------+
  //  |dept     |id |salary|dense_rank|
  //  +---------+---+------+----------+
  //  |develop  |7  |4200  |1         |
  //  |develop  |9  |4500  |2         |
  //  |develop  |10 |5200  |3         |
  //  |develop  |11 |5200  |3         |
  //  |develop  |8  |6000  |4         |
  //  |sales    |3  |4800  |1         |
  //  |sales    |4  |4800  |1         |
  //  |sales    |1  |5000  |2         |
  //  |personnel|5  |3500  |1         |
  //  |personnel|2  |3900  |2         |
  //  +---------+---+------+----------+

  // 3. Row Number(row_number): This function will assign the row number within the window. If 2 rows will have
  // the same value for ordering column, it is non-deterministic which row number will be assigned to each row
  // with same value.
  val rowNumberDF1 = employeeDS
    .withColumn("row_number", row_number().over(windowSpecification1))
  rowNumberDF1.show(false)
  val rowNumberDF2 = employeeDS
    .withColumn("dense_rank", row_number() over Window.partitionBy("dept").orderBy("salary") desc)
  rowNumberDF1.show(false)

  //using sql
  spark
    .sql("select dept, id, salary, row_number() over (PARTITION BY dept ORDER BY salary) as row_number from employee")
    .show(false)

  //  +---------+---+------+----------+
  //  |dept     |id |salary|row_number|
  //  +---------+---+------+----------+
  //  |develop  |7  |4200  |1         |
  //  |develop  |9  |4500  |2         |
  //  |develop  |10 |5200  |3         |
  //  |develop  |11 |5200  |4         |
  //  |develop  |8  |6000  |5         |
  //  |sales    |3  |4800  |1         |
  //  |sales    |4  |4800  |2         |
  //  |sales    |1  |5000  |3         |
  //  |personnel|5  |3500  |1         |
  //  |personnel|2  |3900  |2         |
  //  +---------+---+------+----------+

    // WITHOUT PARTITION
    val rowNumberDF3 = employeeDS
      .withColumn("row_number", row_number() over Window.orderBy("salary"))
    rowNumberDF3.show(false)
  //using sql
  spark
    .sql("select dept, id, salary, row_number() over (ORDER BY salary) as row_number from employee")
    .show(false)

//  +---------+---+------+----------+
//  |dept     |id |salary|row_number|
//  +---------+---+------+----------+
//  |personnel|5  |3500  |1         |
//  |personnel|2  |3900  |2         |
//  |develop  |7  |4200  |3         |
//  |develop  |9  |4500  |4         |
//  |sales    |3  |4800  |5         |
//  |sales    |4  |4800  |6         |
//  |sales    |1  |5000  |7         |
//  |develop  |10 |5200  |8         |
//  |develop  |11 |5200  |9         |
//  |develop  |8  |6000  |10        |
//  +---------+---+------+----------+

  val maxId = 100 // DB
  rowNumberDF3
    .map{ x =>
      println(x.getInt(3))
      val newID = maxId + x.getInt(3)
      x.getLong(1).toInt + newID
    }.show(false)

  val secondHighestSalaryDF1 = employeeDS
    .withColumn("rank", rank() over Window.partitionBy("dept").orderBy("salary"))

  val secondHighestSalaryDF2 = employeeDS
    .withColumn("dense_rank", dense_rank() over Window.partitionBy("dept").orderBy("salary"))

  secondHighestSalaryDF1.show(false)

//  +---------+---+------+----+
//  |dept     |id |salary|rank|
//  +---------+---+------+----+
//  |develop  |7  |4200  |1   |
//  |develop  |9  |4500  |2   |
//  |develop  |10 |5200  |3   |
//  |develop  |11 |5200  |3   |
//  |develop  |11 |5200  |3   |
//  |develop  |8  |6000  |6   |
//  |sales    |3  |4800  |1   |
//  |sales    |4  |4800  |1   |
//  |sales    |1  |5000  |3   |
//  |personnel|5  |3500  |1   |
//  |personnel|2  |3900  |2   |
//  +---------+---+------+----+
  secondHighestSalaryDF2.show(false)

//  +---------+---+------+----------+
//  |dept     |id |salary|dense_rank|
//  +---------+---+------+----------+
//  |develop  |7  |4200  |1         |
//  |develop  |9  |4500  |2         |
//  |develop  |10 |5200  |3         |
//  |develop  |11 |5200  |3         |
//  |develop  |11 |5200  |3         |
//  |develop  |8  |6000  |4         |
//  |sales    |3  |4800  |1         |
//  |sales    |4  |4800  |1         |
//  |sales    |1  |5000  |2         |
//  |personnel|5  |3500  |1         |
//  |personnel|2  |3900  |2         |
//  +---------+---+------+----------+
  val secondHS = secondHighestSalaryDF2
  .filter(col("dense_rank") === 2)

  secondHS.show(false)
//  +---------+---+------+----------+
//  |dept     |id |salary|dense_rank|
//  +---------+---+------+----------+
//  |develop  |9  |4500  |2         |
//  |sales    |1  |5000  |2         |
//  |personnel|2  |3900  |2         |
//  +---------+---+------+----------+

  val secondHSWithOutPartitionDF = employeeDS
    .withColumn("dense_rank", dense_rank() over Window.orderBy("salary"))

  secondHSWithOutPartitionDF.show(false)

//  WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
//  +---------+---+------+----------+
//  |dept     |id |salary|dense_rank|
//  +---------+---+------+----------+
//  |personnel|5  |3500  |1         |
//  |personnel|2  |3900  |2         |
//  |develop  |7  |4200  |3         |
//  |develop  |9  |4500  |4         |
//  |sales    |3  |4800  |5         |
//  |sales    |4  |4800  |5         |
//  |sales    |1  |5000  |6         |
//  |develop  |10 |5200  |7         |
//  |develop  |11 |5200  |7         |
//  |develop  |11 |5200  |7         |
//  |develop  |8  |6000  |8         |
//  +---------+---+------+----------+

  secondHSWithOutPartitionDF
    .filter(col("dense_rank") === 2)
    .show(false)

//  +---------+---+------+----------+
//  |dept     |id |salary|dense_rank|
//  +---------+---+------+----------+
//  |personnel|2  |3900  |2         |
//  +---------+---+------+----------+

}
