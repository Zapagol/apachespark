package apache.zap.spark.core.problems

import org.apache.spark.sql.functions._

object MostPopularMovie {

  private val movieIdResource = this.getClass.getResource("/TestData/movie_id.csv").toString
  private val movieNameResource = this.getClass.getResource("/TestData/movie_name.csv").toString

  case class MovieId(user_id: Int, movie_id: Int, rating: Int, timestamp: String)
  case class MovieName(movie_id: Int, movie_name: String)

  def main(args: Array[String]): Unit ={
    mostpopularMoviewId
    mostpopularMovie
  }

  def mostpopularMoviewId(): Unit ={
    import spark.implicits._

    val movieIdData = spark
      .read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("sep", "\t")
      .csv(movieIdResource)
      .map{row =>
        MovieId(row.getString(0).toInt,
          row.getString(1).toInt,
          row.getString(2).toInt,
          row.getString(3)
        )
      }

    movieIdData
      .groupBy($"movie_id")
      .count()
      //.filter($"movie_id" === 481)
      .show(false)
  }

  def mostpopularMovie(): Unit ={
    import spark.implicits._

    val movieIdData = spark
      .read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("sep", "\t")
      .csv(movieIdResource)
      .map{row =>
        MovieId(row.getString(0).toInt,
          row.getString(1).toInt,
          row.getString(2).toInt,
          row.getString(3)
        )
      }

    val movieNames = spark
      .read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("sep", "|")
      .csv(movieNameResource)
      .map{row =>
        MovieName(row.getString(0).toInt,
          row.getString(1)
        )
      }

    val movieId = movieIdData
      .groupBy($"movie_id")
      .count()
      .withColumnRenamed("count", "total_count")

    movieId.as("id")
      .join(movieNames.as("m"), $"id.movie_id" === $"m.movie_id", "inner")
      .select($"movie_name", $"total_count")
      .sort($"total_count".desc)
      .show(false)

  }
}
