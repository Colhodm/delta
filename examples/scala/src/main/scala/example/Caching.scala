package example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, SQLContext}
import io.delta.tables._

import org.apache.spark.sql.functions._
import org.apache.commons.io.FileUtils
import java.io.File

object Caching {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Quickstart")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()


    val file = new File("/tmp/delta-table")
    if (file.exists()) FileUtils.deleteDirectory(file)
    // Create a table
    println("Creating a table")
    val path = file.getCanonicalPath
    var data = spark.range(0, 100)
    data.write.format("delta").save(path)

    // Read table
    val deltaTable = DeltaTable.forPath(path)
    println("Reading table")
    for( a <- 1 to 10){ 
      val r = new scala.util.Random
      val r1 = r.nextInt(100)
      val t0 = System.nanoTime()

        println(deltaTable.readRow((r1).toString),r1)

      val t1 = System.nanoTime()

      println("Elapsed time: " + (t1 - t0) + "ns")
    }
    // Cleanup
    FileUtils.deleteDirectory(file)
    
    spark.stop()
  }
}
