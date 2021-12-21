package apache.zap.spark.core.sparksql

import org.apache.spark.sql.functions._
object AggregateFunctions extends App {

  //Football problem
  import spark.implicits._

  val footBallDf = Seq(
    ("Messi", 30, 25, 2010),
    ("Ronaldo", 32, 15, 2010),
    ("Neymer", 20, 30, 2010),
    ("Messi", 40, 35, 2011),
    ("Ronaldo", 20, 15, 2011),
  ).toDF("name", "goals", "assists", "season")

  footBallDf.show(false)
//  +-------+-----+-------+------+
//  |name   |goals|assists|season|
//  +-------+-----+-------+------+
//  |Messi  |30   |25     |2010  |
//  |Ronaldo|32   |15     |2010  |
//  |Neymer |20   |30     |2010  |
//  |Messi  |40   |35     |2011  |
//  |Ronaldo|20   |15     |2011  |
//  +-------+-----+-------+------+

  //Find total goals scored each player - `col`
  val totalGoals = footBallDf
    .groupBy(col("name"))
    .agg(sum(col("goals")).as("total_goals"))
    .orderBy(col("total_goals").desc)
  totalGoals.show()
//  +-------+-----------+
//  |   name|total_goals|
//  +-------+-----------+
//  |  Messi|         70|
//  |Ronaldo|         52|
//  | Neymer|         20|
//  +-------+-----------+

  //Average number of goals and assists for each player in 2010 - '$'
  val averageGoalsAssistsIn2010 = footBallDf
    .filter($"season" === 2010)
    .groupBy($"name")
    .agg(avg($"goals").as("avg_goals"), avg($"assists").as("avg_assists"))
    .orderBy($"avg_goals".desc)
  averageGoalsAssistsIn2010.show()
//  +-------+---------+-----------+
//  |   name|avg_goals|avg_assists|
//  +-------+---------+-----------+
//  |Ronaldo|     32.0|       15.0|
//  |  Messi|     30.0|       25.0|
//  | Neymer|     20.0|       30.0|
//  +-------+---------+-----------+

  //Highest goals scored
  val highestGoals = footBallDf
    .groupBy("name")
    .agg(max("goals").as("max"))
  highestGoals.show(false)
//  +-------+----------+
//  |name   |max(goals)|
//  +-------+----------+
//  |Neymer |20        |
//  |Ronaldo|32        |
//  |Messi  |40        |
//  +-------+----------+

}
