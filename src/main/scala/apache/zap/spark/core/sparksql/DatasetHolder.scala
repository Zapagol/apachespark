package apache.zap.spark.core.sparksql

import apache.zap.spark.core.sparksql.DSJoins.{LeftJoinedRows, OuterJoinedRows}
import org.apache.spark.sql.{Column, Dataset, Encoder}

object DatasetHolder {
  implicit def toDatasetHolder[T](dataset: Dataset[T]): DatasetHolder[T] = {
    DatasetHolder(dataset)
  }
}

case class InnerJoinedRows[A, B](left: A, right: B)

case class DatasetHolder[T](dataset: Dataset[T]) {
  def leftOuterJoin[U](that: Dataset[U], condition: Column)(
    implicit jrEnc: Encoder[LeftJoinedRows[T, U]]
  ): Dataset[LeftJoinedRows[T, U]] = {
    dataset
      .joinWith(that, condition, "left_outer")
      .map { case (left, right) => LeftJoinedRows(left, Option(right)) }
  }

  def outerJoin[U](that: Dataset[U], condition: Column)(
    implicit jrEnc: Encoder[OuterJoinedRows[T, U]]
  ): Dataset[OuterJoinedRows[T, U]] = {
    dataset
      .joinWith(that, condition, "outer")
      .map { case (left, right) => OuterJoinedRows(Option(left), Option(right)) }
  }
}


