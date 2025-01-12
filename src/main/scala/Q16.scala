package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q16 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    import spark.implicits._
    import schemaProvider._

    val fparts = part.filter(
      ($"p_brand" !== "Brand#45") &&
      !$"p_type".startsWith("MEDIUM POLILSHED") &&
      ($"p_size".isin("49","14","23","45","19","3","36","9")))
      .select($"p_partkey", $"p_brand", $"p_type", $"p_size")

    supplier.filter(!($"s_comment".like(".*Customer.*Complaints.*")))
      // .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", $"ps_suppkey")
      .join(fparts, $"ps_partkey" === fparts("p_partkey"))
      .groupBy($"p_brand", $"p_type", $"p_size")
      .agg(countDistinct($"ps_suppkey").as("supplier_count"))
      .sort($"supplier_count".desc, $"p_brand", $"p_type", $"p_size")
  }

}
