package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q12 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    import spark.implicits._
    import schemaProvider._

    lineitem.filter((
      $"l_shipmode" === "MAIL" || $"l_shipmode" === "SHIP") &&
      $"l_commitdate" < $"l_receiptdate" &&
      $"l_shipdate" < $"l_commitdate" &&
      $"l_receiptdate" >= "1994-01-01" && $"l_receiptdate" < "1995-01-01")
      .join(order, $"l_orderkey" === order("o_orderkey"))
      .select($"l_shipmode", $"o_orderpriority")
      .groupBy($"l_shipmode")
      .agg(
        sum(when($"o_orderpriority" === "1-URGENT" || $"o_orderpriority" === "2-HIGH", 1).otherwise(0)).as("sum_highorderpriority"),
        sum(when($"o_orderpriority" =!= "1-URGENT" || $"o_orderpriority" =!= "2-HIGH", 1).otherwise(0)).as("sum_loworderpriority")
      ).sort($"l_shipmode")
  }

}
