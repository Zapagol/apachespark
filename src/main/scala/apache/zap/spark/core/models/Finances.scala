package apache.zap.spark.core.models

case class Finances(id: Int,
                    hasDebt: Boolean,
                    hasFinancialDepedents: Boolean,
                    hasStudentLoans: Boolean,
                    income: Int
                   )
