package io.github.interestinglab.waterdrop.filter

import com.google.gson
import com.google.gson.Gson
import io.github.interestinglab.waterdrop.apis.BaseFilter
import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{BooleanType, DoubleType, FloatType, IntegerType, LongType, StringType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConverters.mapAsJavaMapConverter

/**
 * @ClassName MapJson
 * @Description TODO
 * @Author honggen.han
 * @Date 2021/11/4 7:37 下午
 * @Version 1.0
 * */
class MapJson extends BaseFilter {
  var conf: Config = ConfigFactory.empty()

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val srcField = conf.getString("source_field")

    val func = udf((s: Map[AnyRef, AnyRef]) => {
      new Gson().toJson(s.asJava)
    })
    df.withColumn(srcField, func(col(srcField)))
  }

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.conf = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = {
    this.conf
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def checkConfig(): (Boolean, String) = {
    if (!conf.hasPath("source_field")) {
      (false, "please specify [source_field] as a non-empty string")
    } else {
      (true, "")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
  }
}
