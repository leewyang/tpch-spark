package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q01 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    import spark.implicits._
    import schemaProvider._

    schemaProvider.lineitem.filter($"l_shipdate" <= "1998-09-02")
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(sum($"l_quantity"), sum($"l_extendedprice"),
        sum(expr("l_extendedprice * (1 - l_discount)")),
        sum(expr("l_extendedprice * (1 - l_discount) * (1 + l_tax)")),
        avg($"l_quantity"), avg($"l_extendedprice"), avg($"l_discount"), count($"l_quantity"))
      .sort($"l_returnflag", $"l_linestatus")
  }
}
