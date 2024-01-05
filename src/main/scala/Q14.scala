package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q14 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    import spark.implicits._
    import schemaProvider._

    part.join(lineitem, $"l_partkey" === $"p_partkey" &&
      $"l_shipdate" >= "1995-09-01" && $"l_shipdate" < "1995-10-01")
      .select($"p_type", expr("l_extendedprice * (1 - l_discount)").as("value"))
      .agg(sum(when($"p_type".startsWith("PROMO"), $"value").otherwise(0)) * 100 / sum($"value"))
  }

}
