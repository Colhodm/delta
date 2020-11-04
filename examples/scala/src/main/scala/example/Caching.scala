/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, SQLContext}
import io.delta.tables._

import org.apache.spark.sql.functions._
import org.apache.commons.io.FileUtils
import java.io.File

object QuickstartCaching {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Caching")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val file = new File("/tmp/delta-table")
    if (file.exists()) FileUtils.deleteDirectory(file)
    
    // Create a table
    println("Creating a table")
    val path = file.getCanonicalPath
    var data = spark.range(0, 5)
    data.write.format("delta").save(path)

    // Read table
    println("Reading the table")
    val df = spark.read.format("delta").load(path)
    df.show()

    // Upsert (merge) new data
    println("Upsert new data")
    val newData = spark.range(0, 20).toDF
    val deltaTable = DeltaTable.forPath(path)

    deltaTable.as("oldData")
      .merge(
        newData.as("newData"),
        "oldData.id = newData.id")
      .whenMatched
      .update(Map("id" -> col("newData.id")))
      .whenNotMatched
      .insert(Map("id" -> col("newData.id")))
      .execute()

    deltaTable.toDF.show()
    println(deltaTable.as("oldData").readRow("id = 5"))

    // Cleanup
    FileUtils.deleteDirectory(file)
    spark.stop()
  }
}
