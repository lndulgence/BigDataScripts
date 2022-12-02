package ML
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._

object App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val data = spark.read.option("header", "true").csv("hdfs://hadoop-master:9000/data/1996_preprocessed.csv")




  }
}