package apache.zap.spark.core.sparksql.datasources

import org.apache.spark.sql.SaveMode

object JsonDS extends App{

  val jsonFile = this.getClass.getResource("/TestData/SparkSQL/jsonFile.json").toString
  val peopleJson = this.getClass.getResource("/TestData/SparkSQL/people.json").toString
  val multiLineJson = this.getClass.getResource("/TestData/SparkSQL/multiLine.json").toString

  val jsonDF = spark.read
    .json(jsonFile)

  jsonDF.show(false)

  val peopleDF = spark.read
    .format("json")
    .load(peopleJson)

  peopleDF.show()

  peopleDF.write
    .mode(SaveMode.Overwrite)
    .json("out_people.json")

  //Multi-Line JSON
  val mutliLineJSONDF = spark.read
    .option("multiline", "true")
    .json(multiLineJson)

  mutliLineJSONDF.show(false)
}
