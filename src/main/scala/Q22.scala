package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q22 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    import spark.implicits._
    import schemaProvider._

    val countryCodes = List("[I1]", "[I2]", "[I3]", "[I4]", "[I5]", "[I6]", "[I7]")
    val filterCondition = $"c_acctbal" > 0.00 && substring($"c_phone", 1, 2).isin(countryCodes: _*)

    val fcustomer = customer.select($"c_acctbal", $"c_custkey", expr("substring(c_phone, 1, 2) as cntrycode"))
      .filter(filterCondition)

    val avg_customer = fcustomer.filter($"c_acctbal" > 0.0)
      .agg(avg($"c_acctbal").as("avg_acctbal"))

    order.groupBy($"o_custkey")
      .agg($"o_custkey").select($"o_custkey")
      .join(fcustomer, $"o_custkey" === fcustomer("c_custkey"), "right_outer")
      .join(avg_customer)
      .filter($"c_acctbal" > $"avg_acctbal")
      .groupBy($"cntrycode")
      .agg(count($"c_acctbal"), sum($"c_acctbal"))
      .sort($"cntrycode")
  }
}
