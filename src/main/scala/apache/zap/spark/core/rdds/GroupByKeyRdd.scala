package apache.zap.spark.core.rdds

import apache.zap.spark.core.models.Purchases

object GroupByKeyRdd {

  def main(args: Array[String]): Unit ={

    val purchaseRdd = sc.parallelize(
      List(Purchases(100,"Geneva",30.0),
        Purchases(300,"Zurich",50.0),
        Purchases(100,"Fri Bourg",40.0),
        Purchases(200,"St. Gallen",100.0),
        Purchases(100,"Lucerne",25.0),
        Purchases(300,"Basel",75.0)
      )
    )

    val purchasesPerMonth = purchaseRdd
      .map(p => (p.customerId, p.price)) //(100, 30.0), (300,50.0)
      .groupByKey() // (100,(30.0,40.0,25.0)), (300,(50.0,75.0))
      .map(p => (p._1,(p._2.size, p._2.sum))) // (100,(3,95.0)), (300,(2,125.0)), (200,(1,100.0))
      .collect()

    purchasesPerMonth.foreach(println)

  }
}
