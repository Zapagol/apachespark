package apache.zap.spark.core.models

case class Demographic(id: Int,
                       age: Int,
                       codingBootCamp: Boolean,
                       country: String,
                       gender: String,
                       isEnthnicMinority: Boolean,
                       servedInMilitary: Boolean
                      )
