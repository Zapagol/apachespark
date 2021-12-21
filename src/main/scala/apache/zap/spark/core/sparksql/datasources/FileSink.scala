package apache.zap.spark.core.sparksql.datasources

import org.apache.spark.sql.{DataFrame, SaveMode}

object FileSink {

  def save(
            dataFrame: DataFrame,
            intermediateLocation: String,
            targetFile: String,
            saveMode: SaveMode = SaveMode.Overwrite,
            charset: String = "UTF-8"
          ): Unit = {
    dataFrame.write
      .option("charset", "windows-31j")
      .mode(saveMode)
      .text(intermediateLocation)

  }
}
