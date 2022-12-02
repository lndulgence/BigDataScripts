package preprocess
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.sql.functions.rand


object App {

  

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val rawdata = spark.read.option("header", "true").csv("hdfs://hadoop-master:9000/data/1996.csv")
      //Uncommenting the below line shows that the var in quotes only takes the value NA
      //rawdata.select(rawdata("CancellationCode")).distinct.show()
    val setnull = udf((x: String)=> if (x=="NA" || x== "") null else x)
      //Drop all forbidden variables. Since the Year var equals 1996 for all rows, it is dropped as well
      //We also drop CancellationCode
    val reduceddata= rawdata.drop("Year","ArrTime","ActualElapsedTime","AirTime","TaxiIn","Diverted","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay","CancellationCode" )
      //in case a possible testing df has null values.
    val nonnulldata=reduceddata.select(reduceddata.columns.map(c => setnull(col(c)).alias(c)): _*).na.drop()
    val reordereddata= nonnulldata.select("Month", "DayofMonth", "DayOfWeek", "DepTime", "CRSDepTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "CRSElapsedTime", "DepDelay", "Origin", "Dest", "Distance", "TaxiOut", "Cancelled", "ArrDelay")
      //reordereddata.show()

      //reordereddata.count() 
      //Long = 5351983 

      //to conduct sql queries:
    reordereddata.createOrReplaceTempView("data")

      //spark.sql("select * from data Where Cancelled=1").count()
      //Long = 128536

      //Cancelled flights represent 2 percent of all flights. These flights are not delayed, so they are useless for prediction
      //We can drop the columns with Cancelled == 1, and drop the column itself.
    val datatotransform = reordereddata.filter("Cancelled==0").toDF().drop("Cancelled")
      //datatotransform.show()
    
      //This very long line creates a HashMap that associates every Airport present in the Origin or Dest columns with an unique id.
    val placedf = datatotransform.select("Origin").distinct.union(datatotransform.select("Dest").distinct).distinct.withColumn("id",monotonicallyIncreasingId).select($"Origin", $"id".cast("int")).as[(String, Int)]
    placedf.write.mode("overwrite").option("header", "true").csv("hdfs://hadoop-master:9000/data/placemap")
    val placemap=placedf.collect.toMap//save map to hdfs for future use, as mappings must remain constant
    val mapplace = udf((x: String) => placemap.get(x))

      //This transformation assigns a unique ID to each Carrier
    val carrierdf = datatotransform.select("UniqueCarrier").distinct.withColumn("id",monotonicallyIncreasingId).select($"UniqueCarrier", $"id".cast("int")).as[(String, Int)]
    val carriermap = carrierdf.collect.toMap //save map to hdfs for future use, as mappings must remain constant
    carrierdf.write.mode("overwrite").option("header", "true").csv("hdfs://hadoop-master:9000/data/placemap")
    val mapcarrier = udf((x:String) => carriermap.get(x))
      //These user defined functions break down the HHMM format columns to two columns containing the hour and minute, respectively.
    val hourtransform=udf((x: String)=> if (x.split("").length==3) x.split("").take(1).toList.mkString("").toInt else if (x.split("").length==4) x.split("").take(2).toList.mkString("").toInt else 0   )
    val minutetransform= udf((x:String) => x.split("").takeRight(2).toList.mkString("").toInt)
  
      //Preprocessing steps related to column transformations
    var preprocess : org.apache.spark.sql.DataFrame = datatotransform.withColumn("Origin_t", mapplace($"Origin")).drop("Origin").withColumn("Dest_t", mapplace($"Dest")).drop("Dest")
    preprocess = preprocess.withColumn("UniqueCarrier_t", mapcarrier($"UniqueCarrier")).drop("UniqueCarrier")
    preprocess = preprocess.withColumn("DepHour", hourtransform($"DepTime")).withColumn("DepMin", minutetransform($"DepTime")).drop("DepTime")
    preprocess  = preprocess.withColumn("CRSDepHour", hourtransform($"CRSDepTime")).withColumn("CRSDepMin", minutetransform($"CRSDepTime")).drop("CRSDepTime")
    preprocess = preprocess.withColumn("CRSArrHour", hourtransform($"CRSArrTime")).withColumn("CRSArrMin", minutetransform($"CRSArrTime")).drop("CRSArrTime")
    preprocess = preprocess.withColumn("CompoundDelay", $"CRSElapsedTime"+$"TaxiOut"+$"DepDelay")
    preprocess = preprocess.select("Month", "DayofMonth", "DayOfWeek", "FlightNum", "CRSElapsedTime", "DepDelay", "Distance", "TaxiOut", "Origin_t", "Dest_t", "UniqueCarrier_t", "DepHour", "DepMin", "CRSDepHour", "CRSDepMin", "CRSArrHour", "CRSArrMin", "CompoundDelay", "ArrDelay")
    val castint = udf(( x: String)=>x.toFloat)
    for(column<-preprocess.columns){
      preprocess=preprocess.withColumn(column, castint(preprocess(column)) )
    }
    val shuffledDF = preprocess.orderBy(rand()).toDF()
    val trainset = shuffledDF.limit((shuffledDF.count()*0.7).toInt)
    val inversedf=shuffledDF.withColumn("id",monotonicallyIncreasingId).orderBy(desc("id"))
    val testset = inversedf.limit((inversedf.count()*0.3).toInt).drop("id")

    trainset.show()

    testset.show()
    
    
 
    trainset.write.mode("overwrite").option("header", "true").csv("hdfs://hadoop-master:9000/data/trainset.csv")
    testset.write.mode("overwrite").option("header", "true").csv("hdfs://hadoop-master:9000/data/testset.csv")

  

      
    



  }
}
