package apache.zap.spark.core.problems

object TextFile{
  private val textFile = this.getClass.getResource("/TestData/textFile.txt").toString

  def main(args: Array[String]): Unit ={
    val textFileDF = sc.textFile(textFile)
    val linesDF = textFileDF
      .map(x => x.split("\n"))

    println(linesDF.count())

    //val wordsDF = linesDF.flatMap()
  }




  //linesDF.collect()

}
