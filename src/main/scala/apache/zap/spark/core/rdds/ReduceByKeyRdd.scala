package apache.zap.spark.core.rdds

import apache.zap.spark.core.models.Purchases

object ReduceByKeyRdd {

  def main(args: Array[String]): Unit ={

    val purchaseRdd = sc.parallelize(
      List(Purchases(100,"Geneva",30.0),
        Purchases(300,"Zurich",50.0),
        Purchases(100,"Fri Bourg",40.0),
        Purchases(200,"St. Gallen",100.0),
        Purchases(100,"Lucerne",25.0),
        Purchases(300,"Basel",75.0)
      ),3
    )

    val purchasesPerMonth = purchaseRdd
      .map(p => (p.customerId, (1, p.price)))
      .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .collect()

    purchasesPerMonth.foreach(println)

  }
}
