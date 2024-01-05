package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q13 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    import spark.implicits._
    import schemaProvider._


    customer.join(order, $"c_custkey" === order("o_custkey")
      && !order("o_comment").like(".*special.*requests.*"), "left_outer")
      .groupBy($"o_custkey")
      .agg(count($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(count($"o_custkey").as("custdist"))
      .sort($"custdist".desc, $"c_count".desc)
  }

}
