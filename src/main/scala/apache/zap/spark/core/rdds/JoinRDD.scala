package apache.zap.spark.core.rdds

object JoinRDD extends App {

  val as= List((101, ("Ruetli", "AG")), (102, ("Brelaz", "DemiTarif")),
    ( 103, ("Gress", "Demi TarifVisa")), ( 104, ( "Schatten", "Demi Tarif")))
  val abos = sc.parallelize(as)

  val ls= List((101, "Bern"), (101, "Thun"), (102, "Lausanne"), (102, "Geneve"),
    (102, "Nyon"), (103, "Zurich"), (103, "St-Gallen"), (103, "Chur"))
  val locations = sc.parallelize(ls)



}
